package replication

//go:generate mockgen -source=facilities.go -destination=facilities_mock.go -package=replication

import (
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
	"time"
)

type Memberlist interface {
	Member(membership.NodeID) (membership.Member, bool)
	Members() []membership.Member
	LastUpdate() time.Time
}

type ConnRegistry interface {
	Local() nodeclient.Conn
	Get(nodeID membership.NodeID) (nodeclient.Conn, error)
}
