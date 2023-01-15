package replication

import (
	"context"
	"errors"
	"sync"
	"time"

	kitlog "github.com/go-kit/log"
	loglevel "github.com/go-kit/log/level"

	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
)

var (
	ErrLevelNotSatisfied = errors.New("consistency level not satisfied")
	ErrNotEnoughReplicas = errors.New("not enough replicas")
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
	Conns      ConnRegistry
	Logger     kitlog.Logger
	MinAcks    int
	Timeout    time.Duration
	ReplicaSet []membership.Member
	AckedIDs   map[membership.NodeID]struct{}
}

type MapFn[T any] func(context.Context, membership.NodeID, nodeclient.Conn, *NodeReply[T])

type ReduceFn[T any] func(cancel func(), nodeID membership.NodeID, reply T, err error) error

func (o Opts[T]) MapReduce(ctx context.Context, mapFn MapFn[T], reduceFn ReduceFn[T]) error {
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

	for i := range o.ReplicaSet {
		member := &o.ReplicaSet[i]

		if !member.IsReacheable() {
			wg.Done()
			continue
		}

		if _, ok := o.AckedIDs[member.ID]; ok {
			wg.Done()
			continue
		}

		go func(nodeID membership.NodeID) {
			defer wg.Done()

			conn, err := o.Conns.Get(nodeID)
			if err != nil {
				loglevel.Warn(
					kitlog.With(o.Logger, "node_id", nodeID),
				).Log("msg", "failed to get connection", "err", err)

				return
			}

			reply := &NodeReply[T]{nodeID: nodeID}
			mapFn(mapCtx, nodeID, conn, reply)

			if reply.err != nil {
				loglevel.Warn(
					kitlog.With(o.Logger, "node_id", nodeID),
				).Log("msg", "failed to replicate", "err", reply.err)
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

	canceled := false
	cancel := func() {
		canceled = true
		cancelMap()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case reply, ok := <-replies:
			if !ok {
				return ErrLevelNotSatisfied
			}

			err := reduceFn(cancel, reply.nodeID, reply.reply, reply.err)

			if canceled {
				return err
			}

			if err != nil {
				cancelMap()
				return err
			}

			if reply.err == nil {
				o.AckedIDs[reply.nodeID] = struct{}{}

				if len(o.AckedIDs) == o.MinAcks {
					return nil
				}
			}
		}
	}
}
