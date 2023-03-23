package service

import (
	"context"
	"fmt"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kivi/internal/generic"
	"github.com/maxpoletaev/kivi/internal/vclock"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeclient"
	"github.com/maxpoletaev/kivi/replication"
	"github.com/maxpoletaev/kivi/replication/proto"
)

func (s *ReplicationServer) validateGetRequest(req *proto.GetRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	if err := s.validateGetRequest(req); err != nil {
		return nil, err
	}

	var (
		members    = s.cluster.Nodes()
		needAcks   = s.readLevel.N(len(members))
		staleIDs   = map[membership.NodeID]struct{}{}
		repliedIDs = map[membership.NodeID]struct{}{}
		allValues  = make([]nodeValue, 0)
	)

	err := replication.Opts[[]nodeclient.VersionedValue]{
		Cluster:    s.cluster,
		ReplicaSet: members,
		AckedIDs:   repliedIDs,
		MinAcks:    needAcks,
		Logger:     s.logger,
		Timeout:    s.readTimeout,
	}.Distribute(
		ctx,
		func(
			ctx context.Context,
			nodeID membership.NodeID,
			conn nodeclient.Conn,
			reply *replication.NodeReply[[]nodeclient.VersionedValue],
		) {
			l := kitlog.With(s.logger, "node_id", nodeID, "key", req.Key)

			level.Debug(l).Log("msg", "getting value from node")

			values, err := conn.StorageGet(ctx, req.Key)
			if err != nil {
				level.Error(l).Log("msg", "failed to get value from node", "err", err)
				reply.Error(err)

				return
			}

			reply.Ok(values)
		},
		func(
			cancel func(),
			nodeID membership.NodeID,
			values []nodeclient.VersionedValue,
			err error,
		) error {
			if len(values) == 0 {
				staleIDs[nodeID] = struct{}{}
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

	for _, id := range merged.StaleReplicas {
		staleIDs[id] = struct{}{}
	}

	if len(staleIDs) > 0 && len(merged.Values) == 1 {
		value := merged.Values[0]

		level.Debug(s.logger).Log(
			"msg", "repairing stale replicas",
			"key", req.Key,
			"stale_replicas", fmt.Sprintf("%v", merged.StaleReplicas),
			"repair_replicas", fmt.Sprintf("%v", generic.MapKeys(staleIDs)),
		)

		var toRepair []membership.Node

		for _, member := range members {
			if _, ok := staleIDs[member.ID]; ok {
				toRepair = append(toRepair, member)
			}
		}

		err := replication.Opts[int]{
			MinAcks:    len(toRepair),
			ReplicaSet: toRepair,
			Cluster:    s.cluster,
			Timeout:    s.writeTimeout,
			Logger:     s.logger,
			Background: true,
		}.Distribute(
			ctx,
			func(ctx context.Context, nodeID membership.NodeID, conn nodeclient.Conn, reply *replication.NodeReply[int]) {
				l := kitlog.With(s.logger, "key", req.Key, "node_id", nodeID, "tomb", value.Tombstone)

				if value.Tombstone {
					if _, err := putTombstone(ctx, conn, req.Key, merged.Version, false); err != nil {
						level.Error(l).Log("msg", "failed to repair", "err", err)
						reply.Error(err)

						return
					}
				}

				if _, err := putValue(ctx, conn, req.Key, value.Data, merged.Version, false); err != nil {
					level.Error(l).Log("msg", "failed to repair", "err", err)
					reply.Error(err)

					return
				}

				level.Debug(l).Log("msg", "replica repaired")

				reply.Ok(0)
			},
			func(cancel func(), nodeID membership.NodeID, res int, err error) error {
				return nil
			},
		)
		if err != nil {
			return nil, err
		}
	}

	return &proto.GetResponse{
		Version: merged.Version,
		Values: func() (pv []*proto.Value) {
			for _, val := range merged.Values {
				if !val.Tombstone {
					pv = append(pv, &proto.Value{Data: val.Data})
				}
			}

			return
		}(),
	}, nil
}

type mergeResult struct {
	Version       string
	Values        []nodeValue
	StaleReplicas []membership.NodeID
}

func mergeVersions(values []nodeValue) (mergeResult, error) {
	valueVersion := make([]*vclock.Vector, len(values))

	// Keep decoded version for each value.
	for i, v := range values {
		var (
			version *vclock.Vector
			err     error
		)

		if version, err = vclock.Decode(v.Version); err != nil {
			return mergeResult{}, fmt.Errorf("invalid version: %w", err)
		}

		valueVersion[i] = version
	}

	// Merge all versions into one.
	mergedVersion := vclock.New()
	for i := 0; i < len(values); i++ {
		mergedVersion = vclock.Merge(mergedVersion, valueVersion[i])
	}

	if len(values) < 2 {
		return mergeResult{
			Version: vclock.MustEncode(mergedVersion),
			Values:  values,
		}, nil
	}

	var staleReplicas []membership.NodeID

	uniqueValues := make(map[string]nodeValue)

	// Identify the highest version among all values.
	highest := valueVersion[0]
	for i := 1; i < len(values); i++ {
		if vclock.Compare(highest, valueVersion[i]) == vclock.Before {
			highest = valueVersion[i]
		}
	}

	for i := 0; i < len(values); i++ {
		value := values[i]

		// Ignore the values that clearly precede the highest version.
		// Keep track of the replicas that returned outdated values.
		if vclock.Compare(valueVersion[i], highest) == vclock.Before {
			staleReplicas = append(staleReplicas, value.NodeID)
			continue
		}

		// Keep unique values only, based on the version.
		if _, ok := uniqueValues[value.Version]; !ok {
			uniqueValues[value.Version] = value
		}
	}

	return mergeResult{
		Version:       vclock.MustEncode(mergedVersion),
		Values:        generic.MapValues(uniqueValues),
		StaleReplicas: staleReplicas,
	}, nil
}
