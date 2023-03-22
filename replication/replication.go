package replication

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	kitlog "github.com/go-kit/log"
	loglevel "github.com/go-kit/log/level"

	"github.com/maxpoletaev/kiwi/internal/grpcutil"
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeapi"
)

var (
	ErrNotEnoughAcks = errors.New("consistency level not satisfied")
)

type NodeReply[T any] struct {
	nodeID membership.NodeID
	err    error
	reply  T
}

func (r *NodeReply[T]) Ok(reply T) {
	r.reply = reply
}

func (r *NodeReply[T]) Error(err error) {
	r.err = err
}

type Opts[T any] struct {
	Cluster    Cluster
	Logger     kitlog.Logger
	ReplicaSet []membership.Node
	AckedIDs   map[membership.NodeID]struct{}
	Timeout    time.Duration
	Background bool
	MinAcks    int
}

// MapFn is called for each node in the replica set. The function should send a
// request to the node and call the reply.Ok() or reply.Error() function with the
// result. The reply is then passed to the reduceFn.
type MapFn[T any] func(context.Context, membership.NodeID, nodeapi.Client, *NodeReply[T])

// ReduceFn is called for each reply. If the function returns an error, the whole
// operation is cancelled. The cancel function can be used to cancel the
// operation without failing it, for example, if the reply is already known to be
// stale.
type ReduceFn[T any] func(cancel func(), nodeID membership.NodeID, reply T, err error) error

// Distribute sends a request to all nodes in the replica set and ensures that
// enough nodes have acknowledged the request. The mapFn is called for each node
// in the replica set, and the reduceFn is called for each reply.
func (o Opts[T]) Distribute(ctx context.Context, mapFn MapFn[T], reduceFn ReduceFn[T]) error {
	if o.AckedIDs == nil {
		o.AckedIDs = make(map[membership.NodeID]struct{})
	}

	if o.Timeout == 0 {
		panic("timeout is not set")
	}

	mapCtx, cancelMap := context.WithTimeout(context.Background(), o.Timeout)
	replies := make(chan *NodeReply[T], len(o.ReplicaSet))

	wg := sync.WaitGroup{}
	wg.Add(len(o.ReplicaSet))

	// Randomize the order of nodes to avoid sending requests to the same node first.
	rand.Seed(time.Now().UnixNano())
	indices := rand.Perm(len(o.ReplicaSet))

	for _, i := range indices {
		member := &o.ReplicaSet[i]

		if !member.IsReachable() {
			wg.Done()
			continue
		}

		if _, ok := o.AckedIDs[member.ID]; ok {
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

			reply := &NodeReply[T]{nodeID: nodeID}
			mapFn(mapCtx, nodeID, conn, reply)

			if reply.err != nil {
				if !errors.Is(reply.err, context.Canceled) && !grpcutil.IsCanceled(reply.err) {
					loglevel.Warn(
						kitlog.With(o.Logger, "node_id", nodeID),
					).Log("msg", "failed to replicate", "err", reply.err)
				}
			}

			replies <- reply
		}(member.ID)
	}

	go func() {
		wg.Wait()
		cancelMap()
		close(replies)
	}()

	// If we already have enough replies, so no need to wait for more.
	if len(o.AckedIDs) >= o.MinAcks {
		return nil
	}

	cancelCalled := false
	cancelCallback := func() {
		cancelCalled = true
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

			err := reduceFn(cancelCallback, reply.nodeID, reply.reply, reply.err)

			if cancelCalled {
				return err
			}

			if err != nil {
				cancelMap()
				return err
			}

			if reply.err == nil {
				o.AckedIDs[reply.nodeID] = struct{}{}

				if len(o.AckedIDs) == o.MinAcks {
					if !o.Background {
						cancelMap()
					}

					return nil
				}
			}
		}
	}
}
