package service

import (
	"context"
	"fmt"

	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kiwi/internal/generic"
	"github.com/maxpoletaev/kiwi/internal/vclock"
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
	"github.com/maxpoletaev/kiwi/replication"
	"github.com/maxpoletaev/kiwi/replication/proto"
	storagepb "github.com/maxpoletaev/kiwi/storage/proto"
)

func (s *ReplicationService) validateGetRequest(req *proto.GetRequest) error {
	if len(req.Key) == 0 {
		return errMissingKey
	}

	return nil
}

func (s *ReplicationService) ReplicatedGet(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	if err := s.validateGetRequest(req); err != nil {
		return nil, err
	}

	var (
		gotAcks   = 0
		members   = s.members.Members()
		needAcks  = s.readLevel.N(len(members))
		repairIDs = map[membership.NodeID]struct{}{}
		allValues = make([]nodeValue, 0)
	)

	err := replication.Opts[[]*storagepb.VersionedValue]{
		Conns:      s.connections,
		ReplicaSet: members,
		MinAcks:    needAcks,
		Logger:     s.logger,
		Timeout:    s.readTimeout,
	}.MapReduce(
		ctx,
		func(
			ctx context.Context,
			nodeID membership.NodeID,
			conn nodeclient.Conn,
			reply *replication.NodeReply[[]*storagepb.VersionedValue],
		) {
			res, err := conn.Get(ctx, &storagepb.GetRequest{Key: req.Key})
			if err != nil {
				reply.Error(err)
				return
			}

			reply.Ok(res.Value)
		},
		func(
			cancel func(),
			nodeID membership.NodeID,
			values []*storagepb.VersionedValue,
			err error,
		) error {
			if err != nil {
				return nil
			}

			for i := range values {
				allValues = append(allValues, nodeValue{nodeID, values[i]})
			}

			if len(values) == 0 {
				repairIDs[nodeID] = struct{}{}
			}

			// Got enough acks, cancel the rest of the requests.
			if gotAcks++; gotAcks >= needAcks {
				cancel()
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
		repairIDs[id] = struct{}{}
	}

	if len(repairIDs) > 0 && len(merged.Values) == 0 {
		level.Debug(s.logger).Log(
			"msg", "repairing stale replicas",
			"key", req.Key,
			"stale_replicas", fmt.Sprintf("%v", merged.StaleReplicas),
			"repair_replicas", fmt.Sprintf("%v", generic.MapKeys(repairIDs)),
		)

		var toRepair []membership.Member
		for _, member := range members {
			if _, ok := repairIDs[member.ID]; ok {
				toRepair = append(toRepair, member)
			}
		}

		var data []byte
		if len(merged.Values) > 0 {
			data = merged.Values[0].Data
		}

		err := replication.Opts[int]{
			MinAcks:    needAcks - gotAcks,
			ReplicaSet: toRepair,
			Conns:      s.connections,
			Logger:     s.logger,
		}.MapReduce(
			ctx,
			func(ctx context.Context, nodeID membership.NodeID, conn nodeclient.Conn, reply *replication.NodeReply[int]) {
				if len(data) == 0 {
					if _, err := putTombstone(ctx, conn, req.Key, merged.Version, false); err != nil {
						reply.Error(err)
						return
					}
				}

				if _, err := put(ctx, conn, req.Key, data, merged.Version, false); err != nil {
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
		var ver *vclock.Vector
		var err error

		if ver, err = vclock.Decode(v.Version); err != nil {
			return mergeResult{}, fmt.Errorf("invalid version: %w", err)
		}

		valueVersion[i] = ver
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
