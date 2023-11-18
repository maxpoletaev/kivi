package replication

import (
	"context"
	"fmt"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeapi"
	"github.com/maxpoletaev/kivi/replication/consistency"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

type OpGetResult struct {
	Version string
	Values  [][]byte
}

type OpGet struct {
	Logger  kitlog.Logger
	Timeout time.Duration
	Level   consistency.Level
	Cluster membership.Cluster
}

func (op *OpGet) Do(ctx context.Context, key string) (*OpGetResult, error) {
	var (
		members    = op.Cluster.Nodes()
		allValues  = make([]nodeValue, 0)
		needAcks   = op.Level.N(len(members))
		staleNodes = map[membership.NodeID]struct{}{}
		ackedNodes = map[membership.NodeID]struct{}{}
	)

	if countAlive(members) < needAcks {
		return nil, fmt.Errorf("not enough nodes alive")
	}

	err := Replicate[[]*storagepb.VersionedValue]{
		Nodes:      members,
		AckedNodes: ackedNodes,
		MinAcks:    needAcks,
		Logger:     op.Logger,
		Cluster:    op.Cluster,
		Timeout:    op.Timeout,
	}.Do(
		ctx,
		func(ctx context.Context, nodeID membership.NodeID, conn *nodeapi.Client) ([]*storagepb.VersionedValue, error) {
			res, err := conn.Storage.Get(ctx, &storagepb.GetRequest{
				Key: key,
			})

			if err != nil {
				return nil, err
			}

			return res.Value, nil
		},
		func(abort func(), nodeID membership.NodeID, values []*storagepb.VersionedValue, err error) error {
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

	for _, id := range merged.staleNodes {
		staleNodes[id] = struct{}{}
	}

	if len(staleNodes) > 0 && len(merged.values) == 1 {
		value := merged.values[0]

		level.Debug(op.Logger).Log(
			"msg", "repairing stale replicas",
			"key", key,
			"stale_replicas", fmt.Sprintf("%v", merged.staleNodes),
			"repair_replicas", fmt.Sprintf("%v", maps.Keys(staleNodes)),
		)

		var toRepair []membership.Node

		for _, member := range members {
			if _, ok := staleNodes[member.ID]; ok {
				toRepair = append(toRepair, member)
			}
		}

		err = Replicate[int]{
			MinAcks:    len(toRepair),
			Nodes:      toRepair,
			Cluster:    op.Cluster,
			Timeout:    op.Timeout,
			Logger:     op.Logger,
			Background: true,
		}.Do(
			ctx,
			func(ctx context.Context, nodeID membership.NodeID, conn *nodeapi.Client) (int, error) {
				if value.Tombstone {
					if _, err := putTombstone(ctx, conn, key, merged.version, false); err != nil {
						return 0, err
					}
				}

				if _, err := putValue(ctx, conn, key, value.Data, merged.version, false); err != nil {
					return 0, err
				}

				return 0, nil
			},
			func(abort func(), nodeID membership.NodeID, res int, err error) error {
				if grpcutil.ErrorCode(err) == codes.AlreadyExists {
					abort()
				}

				return nil
			},
		)

		if err != nil {
			return nil, err
		}
	}

	switch {
	case len(merged.values) == 0:
		return &OpGetResult{
			Version: merged.version,
		}, nil

	default:
		values := make([][]byte, len(merged.values))
		for i, val := range merged.values {
			values[i] = val.Data
		}

		return &OpGetResult{
			Version: merged.version,
			Values:  values,
		}, nil
	}
}
