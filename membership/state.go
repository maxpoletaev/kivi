package membership

import (
	"fmt"
	"time"

	"github.com/maxpoletaev/kivi/internal/rolling"
)

type State struct {
	SourceID NodeID
	Nodes    []Node
}

// ApplyState merges the given nodes with the current cluster state and returns
// the list of all nodes in the cluster after the merge.
func (cl *Cluster) ApplyState(state State) State {
	cl.mut.Lock()
	defer cl.mut.Unlock()

	for _, next := range state.Nodes {
		curr, ok := cl.nodes[next.ID]
		if !ok {
			cl.nodes[next.ID] = next
			continue
		}

		// Preserve the local address.
		if len(curr.LocalAddr) > 0 {
			next.LocalAddr = curr.LocalAddr
		}

		// There is a newer node with the same ID as ours. But if we are not dead yet,
		// meaning something is very broken, better to panic.
		if next.ID == cl.selfID && next.RunID > curr.RunID {
			panic(fmt.Sprintf("node ID collision: %d", next.ID))
		}

		// In case node is restarted, it’s generation number is reset, but the run ID
		// will be higher. In this case, the node must be replaced with the new one.
		if next.RunID > curr.RunID {
			cl.nodes[next.ID] = next
			continue
		}

		// Once node has left the cluster, all nodes must see it as left despite the
		// generation. This is to prevent the node from being marked as unhealthy by the
		// nodes that haven't been notified about the node leaving.
		if next.Status == StatusLeft {
			next.Gen = rolling.Max(curr.Gen, next.Gen)
			cl.nodes[next.ID] = next
			continue
		}

		switch rolling.Compare(curr.Gen, next.Gen) {
		case rolling.Less:
			// Node with the higher generation is preferred.
			cl.nodes[next.ID] = next
		case rolling.Equal:
			// In case of conflict, the worst status is preferred.
			if next.Status.WorseThan(curr.Status) {
				cl.nodes[next.ID] = next
			}
		}
	}

	if state.SourceID != 0 {
		cl.lastSync[state.SourceID] = time.Now()
	}

	nodes := make([]Node, 0, len(cl.nodes))
	for _, node := range cl.nodes {
		nodes = append(nodes, node)
	}

	return State{Nodes: nodes}
}
