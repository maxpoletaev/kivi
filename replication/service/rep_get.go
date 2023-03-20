package service

import (
	"context"
	"fmt"

	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kiwi/internal/generic"
	"github.com/maxpoletaev/kiwi/internal/vclock"
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeapi"
	"github.com/maxpoletaev/kiwi/replication"
	"github.com/maxpoletaev/kiwi/replication/proto"
)

func (s *ReplicationServer) validateGetRequest(req *proto.GetRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationServer) ReplicatedGet(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
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

	err := replication.Opts[[]nodeapi.VersionedValue]{
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
			conn nodeapi.Client,
			reply *replication.NodeReply[[]nodeapi.VersionedValue],
		) {
			values, err := conn.Get(ctx, req.Key)
			if err != nil {
				reply.Error(err)
				return
			}

			reply.Ok(values)
		},
		func(
			cancel func(),
			nodeID membership.NodeID,
			values []nodeapi.VersionedValue,
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

	if len(staleIDs) > 0 && len(merged.Values) <= 1 {
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

		var data []byte
		if len(merged.Values) > 0 {
			data = merged.Values[0].Data
		}

		err := replication.Opts[int]{
			MinAcks:    len(toRepair),
			ReplicaSet: toRepair,
			Cluster:    s.cluster,
			Timeout:    s.writeTimeout,
			Logger:     s.logger,
		}.Distribute(
			ctx,
			func(ctx context.Context, nodeID membership.NodeID, conn nodeapi.Client, reply *replication.NodeReply[int]) {
				if len(data) == 0 {
					if _, err := putTombstone(ctx, conn, req.Key, merged.Version, false); err != nil {
						reply.Error(err)
						return
					}
				}

				if _, err := putValue(ctx, conn, req.Key, data, merged.Version, false); err != nil {
					reply.Error(err)
					return
				}

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
		Values: func() []*proto.Value {
			pv := make([]*proto.Value, len(merged.Values))
			for i := range merged.Values {
				pv[i] = &proto.Value{Data: merged.Values[i].Data}
			}

			return pv
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

		if value.Tombstone {
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
