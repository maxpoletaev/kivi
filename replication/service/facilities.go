package service

//go:generate mockgen -source=facilities.go -destination=facilities_mock_test.go -package=service

import (
	"github.com/maxpoletaev/kv/cluster"
	"github.com/maxpoletaev/kv/membership"
)

type Cluster interface {
	Self() membership.Member
	Members() []membership.Member
	SelfConn() cluster.Conn
	Conn(membership.NodeID) (cluster.Conn, error)
}
