package service

import "github.com/maxpoletaev/kivi/membership"

type Cluster interface {
	ApplyState(nodes []membership.Node, sourceID membership.NodeID) []membership.Node
	Nodes() []membership.Node
	StateHash() uint64
}
