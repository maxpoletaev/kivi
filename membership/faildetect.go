package membership

import (
	"context"
	"time"

	"github.com/maxpoletaev/kiwi/internal/generic"
)

func (cl *Cluster) startDetector() {
	go func() {
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

	cl.logger.Log("msg", "failure detector loop started")
}

func (cl *Cluster) pickRandomNode() *Node {
	nodes := cl.Nodes()
	generic.Shuffle(nodes)

	for _, node := range nodes {
		if node.ID != cl.selfID {
			return &node
		}
	}

	return nil
}

func (cl *Cluster) setNodeStats(id NodeID, status Status) {
	cl.mut.Lock()
	defer cl.mut.Unlock()

	node, ok := cl.nodes[id]
	if !ok {
		return
	}

	if node.Status != status {
		node.Generation++

		oldStatus := node.Status

		node.Status = status

		cl.nodes[id] = node

		cl.logger.Log(
			"msg", "node status changed",
			"node", id,
			"status", status,
			"old_status", oldStatus,
			"generation", node.Generation,
		)
	}
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
		cl.logger.Log("msg", "failed to connect to node", "node", node.ID, "err", err)
		cl.setNodeStats(node.ID, StatusUnhealthy)
		return
	}

	nodes, err := conn.PullPushState(ctx, toApiNodesInfo(cl.Nodes()))
	if err != nil {
		cl.logger.Log("msg", "failed to pull/push state from node", "node", node.ID, "err", err)
		cl.setNodeStats(node.ID, StatusUnhealthy)
		return
	}

	cl.setNodeStats(node.ID, StatusHealthy)

	cl.ApplyState(fromApiNodesInfo(nodes))
}
