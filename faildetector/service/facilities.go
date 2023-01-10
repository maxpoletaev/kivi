package service

import (
	"github.com/maxpoletaev/kv/clust"
	"github.com/maxpoletaev/kv/membership"
)

type Cluster interface {
	Conn(id membership.NodeID) (clust.Conn, error)
	HasMember(id membership.NodeID) bool
	Self() membership.Member
}
