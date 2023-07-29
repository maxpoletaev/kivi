package membership

type NodeID uint32

// Node represents a single cluster member.
type Node struct {
	ID         NodeID
	RunID      int64
	Name       string
	PublicAddr string
	LocalAddr  string
	Error      string
	Status     Status
	Gen        uint32
}

// IsReachable returns true if the node is reachable.
func (n *Node) IsReachable() bool {
	return n.Status == StatusHealthy
}

// Hash64 returns a 64-bit hash of the node.
func (n *Node) Hash64() uint64 {
	return uint64(n.ID) ^ uint64(n.RunID) ^ uint64(n.Gen)
}
