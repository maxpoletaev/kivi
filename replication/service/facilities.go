package service

//go:generate mockgen -source=facilities.go -destination=facilities_mock_test.go -package=service

import (
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
)

type Memberlist interface {
	Self() membership.Member
	Members() []membership.Member
}

type ConnRegistry interface {
	Get(membership.NodeID) (nodeclient.Conn, error)
}
