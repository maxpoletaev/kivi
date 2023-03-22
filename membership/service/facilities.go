package service

import "github.com/maxpoletaev/kivi/membership"

type Cluster interface {
	ApplyState(state membership.State) membership.State
	Nodes() []membership.Node
}
