package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kivi/internal/generic"
	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeapi"
	"github.com/maxpoletaev/kivi/replication"
	"github.com/maxpoletaev/kivi/replication/consistency"
	"github.com/maxpoletaev/kivi/replication/proto"
)

const (
	defaultReadTimeout      = time.Second * 5
	defaultWriteTimeout     = time.Second * 5
	defaultConsistencyLevel = consistency.Quorum
)

var (
	errLevelNotSatisfied = status.Error(codes.Unavailable, "unable to satisfy the desired consistency level")
	errNotEnoughReplicas = status.Error(codes.FailedPrecondition, "not enough replicas available to satisfy the consistency level")
	errMissingVersion    = status.Error(codes.InvalidArgument, "version is required")
	errMissingKey        = status.Error(codes.InvalidArgument, "key is required")
)

type nodeValue struct {
	NodeID membership.NodeID
	nodeapi.VersionedValue
}

func countAlive(members []membership.Node) (alive int) {
	for i := range members {
		if members[i].IsReachable() {
			alive++
		}
	}

	return
}

func putValue(ctx context.Context, conn nodeapi.Client, key string, value []byte, version string, primary bool) (string, error) {
	resp, err := conn.StoragePut(ctx, key, nodeapi.VersionedValue{
		Version: version,
		Data:    value,
	}, primary)

	if err != nil {
		return "", err
	}

	return resp.Version, nil
}

func putTombstone(ctx context.Context, conn nodeapi.Client, key, version string, primary bool) (string, error) {
	resp, err := conn.StoragePut(ctx, key, nodeapi.VersionedValue{
		Version:   version,
		Tombstone: true,
	}, primary)

	if err != nil {
		return "", err
	}

	return resp.Version, nil
}

type ReplicationService struct {
	proto.UnimplementedReplicationServer

	cluster      membership.Cluster
	logger       kitlog.Logger
	readTimeout  time.Duration
	writeTimeout time.Duration
	readLevel    consistency.Level
	writeLevel   consistency.Level
}

func New(cluster membership.Cluster, logger kitlog.Logger) *ReplicationService {
	return &ReplicationService{
		logger:       logger,
		cluster:      cluster,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
		readLevel:    defaultConsistencyLevel,
		writeLevel:   defaultConsistencyLevel,
	}
}

func validateGetRequest(req *proto.GetRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationService) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	if err := validateGetRequest(req); err != nil {
		return nil, err
	}

	var (
		members    = s.cluster.Nodes()
		needAcks   = s.readLevel.N(len(members))
		staleNodes = map[membership.NodeID]struct{}{}
		ackedNodes = map[membership.NodeID]struct{}{}
		allValues  = make([]nodeValue, 0)
	)

	err := replication.Opts[[]nodeapi.VersionedValue]{
		Cluster:    s.cluster,
		Nodes:      members,
		AckedNodes: ackedNodes,
		MinAcks:    needAcks,
		Logger:     s.logger,
		Timeout:    s.readTimeout,
	}.Distribute(
		ctx,
		func(ctx context.Context, nodeID membership.NodeID, conn nodeapi.Client) ([]nodeapi.VersionedValue, error) {
			l := kitlog.With(s.logger, "node_id", nodeID, "key", req.Key)
			level.Debug(l).Log("msg", "getting value from node")

			res, err := conn.StorageGet(ctx, req.Key)
			if err != nil {
				level.Error(l).Log("msg", "failed to get value from node", "err", err)
				return nil, err
			}

			return res.Versions, nil
		},
		func(abort func(), nodeID membership.NodeID, values []nodeapi.VersionedValue, err error) error {
			if len(values) == 0 {
				staleNodes[nodeID] = struct{}{}
				return nil
			}

			for i := range values {
				allValues = append(allValues, nodeValue{nodeID, values[i]})
			}

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	merged, err := mergeVersions(allValues)
	if err != nil {
		return nil, err
	}

	for _, id := range merged.staleReplicas {
		staleNodes[id] = struct{}{}
	}

	if len(staleNodes) > 0 && len(merged.values) == 1 {
		value := merged.values[0]

		level.Debug(s.logger).Log(
			"msg", "repairing stale replicas",
			"key", req.Key,
			"stale_replicas", fmt.Sprintf("%v", merged.staleReplicas),
			"repair_replicas", fmt.Sprintf("%v", generic.MapKeys(staleNodes)),
		)

		var toRepair []membership.Node

		for _, member := range members {
			if _, ok := staleNodes[member.ID]; ok {
				toRepair = append(toRepair, member)
			}
		}

		err := replication.Opts[int]{
			MinAcks:    len(toRepair),
			Nodes:      toRepair,
			Cluster:    s.cluster,
			Timeout:    s.writeTimeout,
			Logger:     s.logger,
			Background: true,
		}.Distribute(
			ctx,
			func(ctx context.Context, nodeID membership.NodeID, conn nodeapi.Client) (int, error) {
				l := kitlog.With(s.logger, "key", req.Key, "node_id", nodeID, "tomb", value.Tombstone)

				if value.Tombstone {
					if _, err := putTombstone(ctx, conn, req.Key, merged.version, false); err != nil {
						level.Error(l).Log("msg", "failed to repair", "err", err)
						return 0, err
					}
				}

				if _, err := putValue(ctx, conn, req.Key, value.Data, merged.version, false); err != nil {
					level.Error(l).Log("msg", "failed to repair", "err", err)
					return 0, err
				}

				level.Debug(l).Log("msg", "replica repaired")

				return 0, nil
			},
			func(abort func(), nodeID membership.NodeID, res int, err error) error {
				return nil
			},
		)
		if err != nil {
			return nil, err
		}
	}

	return &proto.GetResponse{
		Version: merged.version,
		Values: func() (pv []*proto.Value) {
			for _, val := range merged.values {
				if !val.Tombstone {
					pv = append(pv, &proto.Value{Data: val.Data})
				}
			}

			return
		}(),
	}, nil
}

func validatePutRequest(req *proto.PutRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationService) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if err := validatePutRequest(req); err != nil {
		return nil, err
	}

	var (
		members   = s.cluster.Nodes()
		localConn = s.cluster.LocalConn()
		needAcks  = s.writeLevel.N(len(members))
	)

	// Do not attempt to write if we know in advance that there is not enough alive nodes.
	if countAlive(members) < needAcks {
		return nil, errNotEnoughReplicas
	}

	// The first write goes to the coordinator, which is the local node. The coordinator
	// is responsible for generating the version number, which is then send to the other nodes.
	version, err := putValue(ctx, localConn, req.Key, req.Value.Data, req.Version, true)
	if err != nil {
		return nil, err
	}

	// We already received an ack from the primary node, so skip in the map-reduce operation.
	ackedNodes := make(map[membership.NodeID]struct{})
	ackedNodes[s.cluster.SelfID()] = struct{}{}

	err = replication.Opts[string]{
		MinAcks:    needAcks,
		Nodes:      members,
		AckedNodes: ackedNodes,
		Cluster:    s.cluster,
		Logger:     s.logger,
		Timeout:    s.writeTimeout,
		Background: true,
	}.Distribute(
		ctx,
		func(ctx context.Context, nodeID membership.NodeID, conn nodeapi.Client) (string, error) {
			version, err := putValue(ctx, conn, req.Key, req.Value.Data, version, false)
			if err != nil {
				return "", err
			}

			return version, nil
		},
		func(abort func(), nodeID membership.NodeID, version string, err error) error {
			// Abort the operation if one of the nodes already has a newer version.
			if grpcutil.ErrorCode(err) == codes.AlreadyExists {
				abort()
			}

			return nil
		},
	)

	if err != nil {
		// If we did not receive enough acks, return a special error that will be
		// converted to a Unavailable response. This is done to distinguish between
		// write that failed because of a membership partition and write that failed
		// because of a write quorum not being satisfied.
		if errors.Is(err, replication.ErrNotEnoughAcks) {
			return nil, errLevelNotSatisfied
		}

		return nil, err
	}

	return &proto.PutResponse{
		Acknowledged: int32(len(ackedNodes)),
		Version:      version,
	}, nil
}

func validateDeleteRequest(req *proto.DeleteRequest) error {
	if req.Key == "" {
		return errMissingKey
	}

	if req.Version == "" {
		return errMissingVersion
	}

	return nil
}

func (s *ReplicationService) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	if err := validateDeleteRequest(req); err != nil {
		return nil, err
	}

	var (
		members  = s.cluster.Nodes()
		needAcks = s.writeLevel.N(len(members))
	)

	ackedNodes := make(map[membership.NodeID]struct{})
	ackedNodes[s.cluster.SelfID()] = struct{}{}
	localConn := s.cluster.LocalConn()

	version, err := putTombstone(
		ctx, localConn, req.Key, req.Version, true)
	if err != nil {
		return nil, err
	}

	err = replication.Opts[string]{
		Nodes:      members,
		MinAcks:    needAcks,
		AckedNodes: ackedNodes,
		Cluster:    s.cluster,
		Logger:     s.logger,
		Timeout:    s.writeTimeout,
		Background: true,
	}.Distribute(
		ctx,
		func(ctx context.Context, nodeID membership.NodeID, conn nodeapi.Client) (string, error) {
			version, err := putTombstone(ctx, conn, req.Key, version, false)
			if err != nil {
				return "", err
			}

			return version, nil
		},
		func(abort func(), nodeID membership.NodeID, res string, err error) error {
			if err != nil {
				if grpcutil.ErrorCode(err) == codes.AlreadyExists {
					abort()
				}
			}

			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return &proto.DeleteResponse{
		Acknowledged: int32(len(ackedNodes)),
		Version:      version,
	}, nil
}
