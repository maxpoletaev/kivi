package clustering

type ClusterEvent interface {
	isClusterEvent()
}

type NodeJoined struct {
	NodeID     NodeID
	NodeName   string
	GossipAddr string
	ServerAddr string
}

func (*NodeJoined) isClusterEvent() {}

type NodeLeft struct {
	NodeID NodeID
}

func (*NodeLeft) isClusterEvent() {}

type NodeStatusChanged struct {
	SourceID  NodeID
	NodeID    NodeID
	Version   uint64
	Status    Status
	OldStatus Status
}

func (*NodeStatusChanged) isClusterEvent() {}

// Ensure event types stisfy the interface.
var _ ClusterEvent = &NodeStatusChanged{}
var _ ClusterEvent = &NodeJoined{}
var _ ClusterEvent = &NodeLeft{}
