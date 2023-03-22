package replication

//go:generate mockgen -source=facilities.go -destination=facilities_mock.go -package=replication

import (
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeapi"
)

type Cluster interface {
	Nodes() []membership.Node
	SelfID() membership.NodeID
	LocalConn() nodeapi.Client
	Conn(nodeID membership.NodeID) (nodeapi.Client, error)
}
