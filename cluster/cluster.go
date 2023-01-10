package cluster

import (
	"github.com/maxpoletaev/kv/membership"
)

// Cluster is a facade for the cluster members and connections.
type Cluster struct {
	selfID  membership.NodeID
	members MemberRegistry
	conns   *ConnRegistry
}

// New creates a new cluster. The member registry must guarantee that the local
// member (the one with the given SelfID) is always present.
func New(selfID membership.NodeID, ml MemberRegistry, conns *ConnRegistry) *Cluster {
	return &Cluster{
		selfID:  selfID,
		conns:   conns,
		members: ml,
	}
}

// Members returns the list of all cluster members.
func (c *Cluster) Members() []membership.Member {
	return c.members.Members()
}

// Member returns the member by its ID.
func (c *Cluster) Member(id membership.NodeID) (membership.Member, bool) {
	return c.members.Member(id)
}

// HasMember returns true if the member with the given ID is in the cluster.
func (c *Cluster) HasMember(id membership.NodeID) bool {
	return c.members.HasMember(id)
}

// Conn returns the connection to the member with the given ID. If the connection
// was not found, it is created. If the connection is not established, an error is
// returned.
func (c *Cluster) Conn(id membership.NodeID) (Conn, error) {
	return c.conns.Get(id)
}

// Self returns the local member which represents the current node.
// The member registry must guarantee that the local member is always present.
func (c *Cluster) Self() membership.Member {
	member, ok := c.members.Member(c.selfID)
	if !ok {
		panic("local member is not on the member list")
	}

	return member
}

// SelfConn returns the connection to the local GRPC server. Since the local
// member is always present in the cluster, the connection is always found.
// The connection is established through the loopback interface, so it is
// guaranteed to be available.
func (c *Cluster) SelfConn() Conn {
	conn, err := c.conns.Get(c.selfID)
	if err != nil {
		panic("not connected to self")
	}

	return conn
}
