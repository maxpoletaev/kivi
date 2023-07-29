package membership

import (
	"fmt"
	"time"
)

func (cl *SWIMCluster) StateHash() uint64 {
	cl.mut.RLock()
	defer cl.mut.RUnlock()

	return cl.stateHash
}

// ApplyState merges the given nodes with the current cluster state and returns
// the list of all nodes in the cluster after the merge.
func (cl *SWIMCluster) ApplyState(nodes []Node, sourceID NodeID) []Node {
	cl.mut.Lock()
	defer cl.mut.Unlock()

	for _, next := range nodes {
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
			if curr.Gen > next.Gen {
				next.Gen = curr.Gen
			}
			cl.nodes[next.ID] = next
			continue
		}

		if curr.Gen < next.Gen {
			// Node with the higher generation is preferred.
			cl.nodes[next.ID] = next
		} else if curr.Gen == next.Gen {
			// In case of conflict, the worst status is preferred.
			if next.Status.WorseThan(curr.Status) {
				cl.nodes[next.ID] = next
			}
		}
	}

	if sourceID != 0 {
		cl.lastSync[sourceID] = time.Now()
	}

	cl.stateHash = 0
	for _, node := range cl.nodes {
		cl.stateHash ^= node.Hash64()
	}

	nodes = make([]Node, 0, len(cl.nodes))
	for _, node := range cl.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}
