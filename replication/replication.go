package replication

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	kitlog "github.com/go-kit/log"
	loglevel "github.com/go-kit/log/level"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeapi"
)

var (
	ErrNotEnoughAcks = errors.New("consistency level not satisfied")
)

type nodeReply[T any] struct {
	nodeID membership.NodeID
	err    error
	reply  T
}

type Opts[T any] struct {
	// Cluster is the cluster to use for node discovery and connection management.
	Cluster membership.Cluster
	// Logger is used to log errors and debug information.
	Logger kitlog.Logger
	// Nodes is the set of nodes to send the request to. The nodes are expected
	// to be alive, but the operation will not fail if some of them are not.
	Nodes []membership.Node
	// AckedNodes is a set of node ids that have acknowledged the request, after the
	// operation is complete. You can put the primary node here in advance to skip it
	// during the map phase.
	AckedNodes map[membership.NodeID]struct{}
	// Timeout is the maximum amount of time for each node to respond.
	Timeout time.Duration
	// MinAcks is the minimum number of nodes that must acknowledge the request to
	// consider it successful. It not, the operation will return the ErrNotEnoughAcks error.
	// If MinAcks is 0, the operation will return immediately after the map phase.
	MinAcks int
	// Background indicates whether the operation should continue to run in
	// background for the remaining nodes after the minimum number of acknowledgments
	// has been received.
	Background bool
}

// MapFn is called for each node in the replica set. The function should send a
// request to the node and then either return the result or an error. If the
// error is nil, the node is considered to have acknowledged the request.
type MapFn[T any] func(context.Context, membership.NodeID, nodeapi.Client) (T, error)

// ReduceFn is called for each reply. If the function returns an error, the whole
// operation is aborted and the error is propagated to the caller. The abort function
// can be called to manually abort the operation based on nodes' replies.
type ReduceFn[T any] func(abort func(), nodeID membership.NodeID, ret T, err error) error

// Distribute sends a request to all nodes in the replica set in parallel and
// ensures that enough nodes have acknowledged the request. The mapFn is called
// for each node in the replica set, and the reduceFn is called for each reply.
// The function blocks until the minimum number of acknowledgments has been
// received or the operation is aborted either by canceling the context or
// calling the abort function in the reduceFn.
func (o Opts[T]) Distribute(ctx context.Context, mapFn MapFn[T], reduceFn ReduceFn[T]) error {
	if o.AckedNodes == nil {
		o.AckedNodes = make(map[membership.NodeID]struct{})
	}

	if o.Timeout == 0 {
		panic("timeout is not set")
	}

	mapCtx, cancelMap := context.WithTimeout(context.Background(), o.Timeout)
	replies := make(chan nodeReply[T], len(o.Nodes))

	wg := sync.WaitGroup{}
	wg.Add(len(o.Nodes))

	// Randomize the order of nodes to avoid sending requests to the same node first.
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	indices := rnd.Perm(len(o.Nodes))

	for _, i := range indices {
		member := &o.Nodes[i]

		if !member.IsReachable() {
			wg.Done()
			continue
		}

		if _, ok := o.AckedNodes[member.ID]; ok {
			wg.Done()
			continue
		}

		go func(nodeID membership.NodeID) {
			defer wg.Done()

			conn, err := o.Cluster.Conn(nodeID)
			if err != nil {
				loglevel.Warn(
					kitlog.With(o.Logger, "node_id", nodeID),
				).Log("msg", "failed to get connection", "err", err)

				return
			}

			ret, err := mapFn(mapCtx, nodeID, conn)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !grpcutil.IsCanceled(err) {
					loglevel.Warn(
						kitlog.With(o.Logger, "node_id", nodeID),
					).Log("msg", "failed to replicate", "err", err)
				}
			}

			replies <- nodeReply[T]{
				nodeID: nodeID,
				reply:  ret,
				err:    err,
			}
		}(member.ID)
	}

	go func() {
		wg.Wait()
		cancelMap()
		close(replies)
	}()

	// If we already have enough replies, no need to wait for more.
	if len(o.AckedNodes) >= o.MinAcks {
		if !o.Background {
			cancelMap()
		}

		return nil
	}

	aborted := false
	abort := func() {
		aborted = true
		cancelMap() // nolint:wsl
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case reply, ok := <-replies:
			if !ok {
				return ErrNotEnoughAcks
			}

			err := reduceFn(abort, reply.nodeID, reply.reply, reply.err)

			if aborted {
				return err
			}

			if err != nil {
				cancelMap()
				return err
			}

			if reply.err == nil {
				o.AckedNodes[reply.nodeID] = struct{}{}

				if len(o.AckedNodes) == o.MinAcks {
					if !o.Background {
						cancelMap()
					}

					return nil
				}
			}
		}
	}
}
