package replication

//go:generate mockgen -source=facilities.go -destination=facilities_mock.go -package=replication

import (
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeclient"
)

type ConnRegistry interface {
	Get(nodeID membership.NodeID) (nodeclient.Conn, error)
	Local() nodeclient.Conn
}
