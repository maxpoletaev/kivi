package service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeclient"
	"github.com/maxpoletaev/kivi/replication"
	"github.com/maxpoletaev/kivi/replication/proto"
)

func (s *ReplicationServer) validatePutRequest(req *proto.PutRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if err := s.validatePutRequest(req); err != nil {
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
	ackedIDs := make(map[membership.NodeID]struct{})
	ackedIDs[s.cluster.SelfID()] = struct{}{}

	err = replication.Opts[string]{
		MinAcks:    needAcks,
		ReplicaSet: members,
		AckedIDs:   ackedIDs,
		Cluster:    s.cluster,
		Logger:     s.logger,
		Timeout:    s.writeTimeout,
		Background: true,
	}.Distribute(
		ctx,
		func(ctx context.Context, nodeID membership.NodeID, conn nodeclient.Conn, reply *replication.NodeReply[string]) {
			version, err := putValue(ctx, conn, req.Key, req.Value.Data, version, false)
			if err != nil {
				reply.Error(err)
				return
			}

			reply.Ok(version)
		},
		func(cancel func(), nodeID membership.NodeID, version string, err error) error {
			// Cancel the whole write if one of the replicas already has a newer value.
			if grpcutil.ErrorCode(err) == codes.AlreadyExists {
				cancel()
			}

			return nil
		},
	)

	if err != nil {
		// If we did not receive enough acks, return a special error that will be
		// converted to a Unavailable response. This is done to distinguish between
		// a write that failed because of a membership partition and a write that failed
		// because of a write quorum not being satisfied.
		if errors.Is(err, replication.ErrNotEnoughAcks) {
			return nil, errLevelNotSatisfied
		}

		return nil, err
	}

	return &proto.PutResponse{
		Version: version,
	}, nil
}

func putValue(
	ctx context.Context,
	conn nodeclient.Conn,
	key string,
	value []byte,
	version string,
	primary bool,
) (string, error) {
	resp, err := conn.StoragePut(ctx, key, nodeclient.VersionedValue{
		Version: version,
		Data:    value,
	}, primary)

	if err != nil {
		return "", err
	}

	return resp.Version, nil
}
