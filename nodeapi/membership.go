package nodeapi

import (
	"context"
	"time"
)

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
	Gen    uint32
	Error  string
	RunID  int64
}

type PingResult struct {
	Took    time.Duration
	Status  NodeStatus
	Message string
}

type membershipClient interface {
	// Ping returns the hash of the cluster state.
	Ping(ctx context.Context) (uint64, error)
	// PullPushState exchanges the state of the cluster with the remote node.
	PullPushState(ctx context.Context, nodes []NodeInfo) ([]NodeInfo, error)
	// PingIndirect pings the target node indirectly via the current node.
	PingIndirect(ctx context.Context, target NodeID, timeout time.Duration) (PingResult, error)
}
