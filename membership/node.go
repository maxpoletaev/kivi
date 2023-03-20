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

// Node represents a single cluster member.
type Node struct {
	ID           NodeID
	RunID        uint32
	Name         string
	Address      string
	LocalAddress string
	Status       Status
	Generation   int32
}

// Update updates the node with the values from other node.
// The need for this method is that we don't want LocalAddress overridden.
func (n *Node) Update(other *Node) {
	n.RunID = other.RunID
	n.Name = other.Name
	n.Address = other.Address
	n.Status = other.Status
	n.Generation = other.Generation
}

// IsReachable returns true if the node is reachable.
func (n *Node) IsReachable() bool {
	return n.Status == StatusHealthy
}
