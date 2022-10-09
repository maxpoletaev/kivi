package clustering

// NodeID is a unique cluster node identifier.
type NodeID uint32

// Member contains info and status of a cluster member.
type Member struct {
	ID              NodeID
	Name            string
	ServerAddr      string
	GossipAddr      string
	LocalServerAddr string
	Version         uint64
	Status          Status
}

// Node is a cluster member with an established GRPC connection.
type Node struct {
	Member
	Local bool
	*Connection
}
