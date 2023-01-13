package service

import (
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
)

type Memberlist interface {
	Self() membership.Member
	Members() []membership.Member
	SetStatus(id membership.NodeID, status membership.Status) (membership.Status, error)
}

type ConnRegistry interface {
	Get(id membership.NodeID) (nodeclient.Conn, error)
}
