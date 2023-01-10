package service

import (
	"github.com/maxpoletaev/kv/cluster"
	"github.com/maxpoletaev/kv/membership"
)

type Cluster interface {
	Conn(id membership.NodeID) (cluster.Conn, error)
	HasMember(id membership.NodeID) bool
	Self() membership.Member
}
