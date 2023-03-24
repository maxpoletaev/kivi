package nodeclient

import "context"

type NodeID uint32

type NodeStatus uint8

const (
	NodeStatusHealthy NodeStatus = iota + 1
	NodeStatusUnhealthy
	NodeStatusLeft
)

type NodeInfo struct {
	ID     NodeID
	Name   string
	Status NodeStatus
	Addr   string
	Gen    int32
	Error  string
	RunID  int64
}

type clusterClient interface {
	// PullPushState exchanges the state of the cluster with the remote node.
	PullPushState(ctx context.Context, nodes []NodeInfo) ([]NodeInfo, error)

	// GetStateHash returns the hash of the cluster state.
	GetStateHash(ctx context.Context) (uint64, error)
}
