package nodeapi

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
	Status NodeStatus
	Addr   string
	Gen    int32
	Error  string
	RunID  int64
}

type clusterClient interface {
	PullPushState(ctx context.Context, nodes []NodeInfo) ([]NodeInfo, error)
}
