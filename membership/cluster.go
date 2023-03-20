package membership

import (
	"context"
	"fmt"
	"sync"
	"time"

	kitlog "github.com/go-kit/log"

	"github.com/maxpoletaev/kiwi/internal/generic"
	"github.com/maxpoletaev/kiwi/internal/overflow"
	"github.com/maxpoletaev/kiwi/nodeapi"
)

type Cluster struct {
	mut           sync.RWMutex
	selfID        NodeID
	nodes         map[NodeID]Node
	connections   map[NodeID]nodeapi.Client
	waiting       *generic.SyncMap[NodeID, chan struct{}]
	dialer        nodeapi.Dialer
	logger        kitlog.Logger
	dialTimeout   time.Duration
	probeTimeout  time.Duration
	probeInterval time.Duration
	gcInterval    time.Duration
	stop          chan struct{}
}

func NewCluster(localNode Node, conf Config) *Cluster {
	nodes := make(map[NodeID]Node, 1)
	localNode.Status = StatusHealthy
	nodes[localNode.ID] = localNode

	return &Cluster{
		nodes:         nodes,
		selfID:        localNode.ID,
		connections:   make(map[NodeID]nodeapi.Client),
		waiting:       new(generic.SyncMap[NodeID, chan struct{}]),
		dialer:        conf.Dialer,
		logger:        conf.Logger,
		probeTimeout:  conf.ProbeTimeout,
		probeInterval: conf.ProbeInterval,
		dialTimeout:   conf.DialTimeout,
		gcInterval:    conf.GCInterval,
		stop:          make(chan struct{}),
	}
}

// Start schedules background tasks for managing the cluster state, such as
// probing nodes and garbage collecting unreachable nodes.
func (cl *Cluster) Start() {
	cl.startDetector()
	cl.startGC()
}

// SelfID returns the ID of the current node.
func (cl *Cluster) SelfID() NodeID {
	return cl.selfID
}

// Self returns the current node.
func (cl *Cluster) Self() Node {
	cl.mut.RLock()
	defer cl.mut.RUnlock()

	return cl.nodes[cl.selfID]
}

// Nodes returns a list of all nodes in the cluster, including the current node,
// and nodes that have recently left the cluster but have not been garbage
// collected yet.
func (cl *Cluster) Nodes() []Node {
	cl.mut.RLock()
	defer cl.mut.RUnlock()

	nodes := make([]Node, 0, len(cl.nodes))
	for _, node := range cl.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// Node returns the node with the given ID, if it exists.
func (cl *Cluster) Node(id NodeID) (Node, bool) {
	cl.mut.RLock()
	defer cl.mut.RUnlock()
	node, ok := cl.nodes[id]

	return node, ok
}

// Leave removes the current node from the cluster. The leave call blocks until
// at least one other node acknowledges the leave request.
func (cl *Cluster) Leave(ctx context.Context) error {
	cl.setNodeStats(cl.selfID, StatusLeft)

	if err := cl.broadcast(ctx); err != nil {
		return fmt.Errorf("broadcast state: %w", err)
	}

	cl.mut.Lock()
	defer cl.mut.Unlock()

	self := cl.nodes[cl.selfID]
	nodes := make(map[NodeID]Node, 1)
	nodes[cl.selfID] = self
	cl.nodes = nodes

	for id, conn := range cl.connections {
		delete(cl.connections, id)

		_ = conn.Close()
	}

	close(cl.stop)

	return nil
}

func (cl *Cluster) broadcast(ctx context.Context) error {
	nodes := cl.Nodes()

	// There are no other nodes in the cluster, so there is nothing to broadcast.
	if len(nodes) == 1 {
		return nil
	}

	wg := sync.WaitGroup{}
	replies := make(chan struct{})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	nodesInfo := make([]nodeapi.NodeInfo, len(nodes))
	for i, node := range nodes {
		nodesInfo[i] = toApiNodeInfo(&node)
	}

	for _, node := range nodes {
		if node.ID == cl.selfID || node.Status != StatusHealthy {
			continue
		}

		wg.Add(1)

		go func(node Node) {
			defer wg.Done()

			conn, err := cl.ConnContext(ctx, node.ID)
			if err != nil {
				return
			}

			if _, err = conn.PullPushState(ctx, nodesInfo); err != nil {
				return
			}

			replies <- struct{}{}
		}(node)
	}

	go func() {
		wg.Wait()
		cancel()
		close(replies)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-replies:
			if !ok {
				return fmt.Errorf("not enough acknoledgements")
			}

			cancel()

			return nil
		}
	}
}

// Join adds the current node to the cluster with the given address.
// All nodes from the remote cluster are added to the local cluster and vice versa.
func (cl *Cluster) Join(ctx context.Context, addr string) error {
	conn, err := cl.dialer(ctx, addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	defer func() {
		_ = conn.Close()
	}()

	nodes, err := conn.PullPushState(ctx, toApiNodesInfo(cl.Nodes()))
	if err != nil {
		return fmt.Errorf("pull push state: %w", err)
	}

	cl.ApplyState(fromApiNodesInfo(nodes))

	return nil
}

// ApplyState merges the given nodes with the current cluster state and returns
// the list of all nodes in the cluster after the merge.
func (cl *Cluster) ApplyState(nodes []Node) []Node {
	cl.mut.Lock()
	defer cl.mut.Unlock()

	for _, node := range nodes {
		curr, ok := cl.nodes[node.ID]
		if !ok {
			cl.nodes[node.ID] = node
			continue
		}

		// Node with the higher generation, or the same generation but with the worst status is preferred.
		switch overflow.CompareInt32(curr.Generation, node.Generation) {
		case overflow.Less:
			curr.Update(&node)
			cl.nodes[node.ID] = curr
		case overflow.Equal:
			if node.Status > curr.Status {
				curr.Update(&node)
				cl.nodes[node.ID] = curr
			}
		}
	}

	nodes = make([]Node, 0, len(cl.nodes))
	for _, node := range cl.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}
