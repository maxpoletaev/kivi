package replication

//go:generate mockgen -source=facilities.go -destination=facilities_mock.go -package=replication

import (
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeclient"
)

type Cluster interface {
	Nodes() []membership.Node
	SelfID() membership.NodeID
	LocalConn() nodeclient.Conn
	Conn(nodeID membership.NodeID) (nodeclient.Conn, error)
}
