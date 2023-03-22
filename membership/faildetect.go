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

	cl.logger.Log("msg", "failure detector loop started")
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
	cl.mut.Lock()
	defer cl.mut.Unlock()

	node, ok := cl.nodes[id]
	if !ok {
		return
	}

	if node.Status != status {
		cl.logger.Log(
			"msg", "node status changed",
			"node_id", id,
			"status", status,
			"error", err,
			"generation", node.Gen,
		)

		node.Status = status
		node.Error = ""
		node.Gen++

		if err != nil {
			node.Error = err.Error()
		}

		cl.nodes[id] = node
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
		cl.setStatus(node.ID, StatusUnhealthy, err)
		return
	}

	nodesInfo, err := conn.PullPushState(ctx, toApiNodesInfo(cl.Nodes()))
	if err != nil {
		cl.setStatus(node.ID, StatusUnhealthy, err)
		return
	}

	cl.setStatus(node.ID, StatusHealthy, nil)

	nodes := fromApiNodesInfo(nodesInfo)

	cl.ApplyState(State{
		SourceID: node.ID,
		Nodes:    nodes,
	})
}
