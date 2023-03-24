package api

//go:generate mockgen -source=facilities.go -destination=facilities_mock_test.go -package=api

import (
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeclient"
)

type Cluster interface {
	LocalConn() nodeclient.Conn
	Nodes() []membership.Node
}
