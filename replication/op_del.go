package replication

import (
	"context"
	"time"

	kitlog "github.com/go-kit/log"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/noderpc"
	"github.com/maxpoletaev/kivi/replication/consistency"
)

type OpDel struct {
	Logger  kitlog.Logger
	Timeout time.Duration
	Level   consistency.Level
	Cluster membership.Cluster
}

type OpDelResult struct {
	Version      string
	Acknowledged int
}

func (s *OpDel) Do(ctx context.Context, key, version string) (*OpDelResult, error) {
	var (
		members  = s.Cluster.Nodes()
		needAcks = s.Level.N(len(members))
	)

	if countAlive(members) < needAcks {
		return nil, ErrUnavailable
	}

	ackedNodes := make(map[membership.NodeID]struct{})
	ackedNodes[s.Cluster.SelfID()] = struct{}{}
	localConn := s.Cluster.LocalConn()

	nextVersion, err := putTombstone(
		ctx, localConn, key, version, true)
	if err != nil {
		return nil, err
	}

	err = Replicate[string]{
		Nodes:      members,
		MinAcks:    needAcks,
		AckedNodes: ackedNodes,
		Cluster:    s.Cluster,
		Logger:     s.Logger,
		Timeout:    s.Timeout,
		Background: true,
	}.Do(
		ctx,
		func(ctx context.Context, nodeID membership.NodeID, conn noderpc.Client) (string, error) {
			version, err := putTombstone(ctx, conn, key, nextVersion, false)
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

	return &OpDelResult{
		Acknowledged: len(ackedNodes),
		Version:      version,
	}, nil
}
