package membership

type NodeID uint32

type Status uint8

const (
	// StatusHealthy is the status of a healthy node.
	StatusHealthy Status = iota + 1

	// StatusUnhealthy is the status of a node that has failed a health check.
	StatusUnhealthy

	// StatusLeft is the status of a node that has left the cluster.
	StatusLeft
)

// String returns the string representation of the status.
func (s Status) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusUnhealthy:
		return "unhealthy"
	case StatusLeft:
		return "left"
	default:
		return ""
	}
}

// WorseThan returns true if the status is worse than the other status.
func (s Status) WorseThan(other Status) bool {
	return s > other
}

// Node represents a single cluster member.
type Node struct {
	ID         NodeID
	RunID      int64
	Name       string
	PublicAddr string
	LocalAddr  string
	Error      string
	Status     Status
	Gen        int32
}

// IsReachable returns true if the node is reachable.
func (n *Node) IsReachable() bool {
	return n.Status == StatusHealthy
}

// Hash64 returns a 64-bit hash of the node.
func (n *Node) Hash64() uint64 {
	return uint64(n.ID) ^ uint64(n.RunID) ^ uint64(n.Gen)
}
