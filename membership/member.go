package membership

import (
	"fmt"
)

// NodeID is a unique cluster node identifier.
type NodeID uint32

func (id NodeID) String() string {
	return fmt.Sprintf("%d", id)
}

type Member struct {
	// ID is the unique identifier of a cluster node.
	ID NodeID
	// RunID is node identifier that is unique on every node restart.
	RunID uint32
	// Name is the unique human-readable name of a cluster node.
	Name string
	// ServerAddr is the address of a GRPC server that is advertised to other nodes.
	ServerAddr string
	// GossipAddr is the address of the gossip port advertised to other nodes.
	GossipAddr string
	// Version number used for conflict resolution. The version is incremented each time the member is updated.
	Version uint64
	// Status is the current status of the node. For now, it is either alive or dead.
	Status Status
}

// IsReachable returns true if the member can be reached by other nodes.
func (m *Member) IsReachable() bool {
	return m.Status == StatusHealthy
}
