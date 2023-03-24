package membership

import (
	"context"
	"time"

	"github.com/maxpoletaev/kivi/internal/generic"
)

func (cl *Cluster) startDetector() {
	cl.wg.Add(1)

	go func() {
		defer cl.wg.Done()

		ticker := time.NewTicker(cl.probeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cl.detectFailures()
			case <-cl.stop:
				return
			}
		}
	}()
}

func (cl *Cluster) pickRandomNode() *Node {
	nodes := cl.Nodes()
	generic.Shuffle(nodes)

	for _, node := range nodes {
		if node.ID != cl.selfID && node.Status != StatusLeft {
			return &node
		}
	}

	return nil
}

func (cl *Cluster) setStatus(id NodeID, status Status, err error) {
	var (
		node Node
		ok   bool
	)

	if withLock(cl.mut.RLocker(), func() {
		node, ok = cl.nodes[id]
	}); !ok || node.Status == status {
		return
	}

	cl.logger.Log(
		"msg", "node status changed",
		"node_id", node.ID,
		"status", status,
		"error", err,
	)

	node.Status = status
	node.Error = ""
	node.Gen++

	if err != nil {
		node.Error = err.Error()
	}

	cl.ApplyState([]Node{node}, 0)
}

func (cl *Cluster) detectFailures() {
	target := cl.pickRandomNode()
	if target == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cl.probeTimeout)
	cl.directProbe(ctx, target)
	cancel()
}

func (cl *Cluster) directProbe(ctx context.Context, node *Node) {
	conn, err := cl.Conn(node.ID)
	if err != nil {
		cl.setStatus(node.ID, StatusUnhealthy, err)
		return
	}

	// Very cheap request that is used to check if the node is actually alive and if
	// there is any difference between the states of the nodes.
	stateHash, err := conn.GetStateHash(ctx)
	if err != nil {
		cl.setStatus(node.ID, StatusUnhealthy, err)
		return
	}

	// In case of a difference between the local and remote state hashes, we pull the
	// full state exchange is performed. This would merge the states of both nodes
	// and make them consistent.
	if stateHash != cl.StateHash() {
		nodesInfo, err := conn.PullPushState(ctx, toApiNodesInfo(cl.Nodes()))
		if err != nil {
			return
		}

		nodes := fromApiNodesInfo(nodesInfo)
		cl.ApplyState(nodes, node.ID)
	}

	cl.setStatus(node.ID, StatusHealthy, nil)
}
