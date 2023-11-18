package replication

import (
	"context"
	"time"

	kitlog "github.com/go-kit/log"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeapi"
	"github.com/maxpoletaev/kivi/replication/consistency"
)

type OpPut struct {
	Logger  kitlog.Logger
	Timeout time.Duration
	Level   consistency.Level
	Cluster membership.Cluster
}

type OpPutResult struct {
	Version      string
	Acknowledged int
}

func (op *OpPut) Do(ctx context.Context, key string, value []byte, version string) (*OpPutResult, error) {
	var (
		members   = op.Cluster.Nodes()
		localConn = op.Cluster.LocalConn()
		needAcks  = op.Level.N(len(members))
	)

	// Do not attempt to write if we know in advance that there is not enough alive nodes.
	if countAlive(members) < needAcks {
		return nil, ErrUnavailable
	}

	// The first write goes to the coordinator, which is the local node. The coordinator
	// is responsible for generating the version number, which is then send to the other nodes.
	version, err := putValue(ctx, localConn, key, value, version, true)
	if err != nil {
		return nil, err
	}

	// We already received an ack from the primary node, so skip in the map-reduce operation.
	ackedNodes := make(map[membership.NodeID]struct{})
	ackedNodes[op.Cluster.SelfID()] = struct{}{}

	err = Replicate[string]{
		MinAcks:    needAcks,
		Nodes:      members,
		AckedNodes: ackedNodes,
		Cluster:    op.Cluster,
		Logger:     op.Logger,
		Timeout:    op.Timeout,
		Background: true,
	}.Do(
		ctx,
		func(ctx context.Context, nodeID membership.NodeID, conn *nodeapi.Client) (string, error) {
			version, err := putValue(ctx, conn, key, value, version, false)
			if err != nil {
				return "", err
			}

			return version, nil
		},
		func(abort func(), nodeID membership.NodeID, version string, err error) error {
			// Abort the operation if one of the nodes already has a newer version.
			if grpcutil.ErrorCode(err) == codes.AlreadyExists {
				return err
			}

			return nil
		},
	)

	if err != nil {
		return nil, err
	}

	return &OpPutResult{
		Acknowledged: len(ackedNodes),
		Version:      version,
	}, nil
}
