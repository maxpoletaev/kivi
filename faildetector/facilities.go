package faildetector

import (
	clusterpkg "github.com/maxpoletaev/kv/cluster"
	"github.com/maxpoletaev/kv/membership"
)

type memberRegistry interface {
	Self() membership.Member
	Members() []membership.Member
	SetStatus(id membership.NodeID, status membership.Status) (membership.Status, error)
}

type connectionRegistry interface {
	Get(id membership.NodeID) (clusterpkg.Client, error)
}
