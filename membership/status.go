package membership

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
