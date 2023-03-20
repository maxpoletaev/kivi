package service

import "github.com/maxpoletaev/kiwi/membership"

type Cluster interface {
	ApplyState(nodes []membership.Node) []membership.Node
	Nodes() []membership.Node
}
