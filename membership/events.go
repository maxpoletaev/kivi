package membership

var (
	_ ClusterEvent = &MemberUpdated{}
	_ ClusterEvent = &MemberJoined{}
	_ ClusterEvent = &MemberLeft{}
)

type ClusterEvent interface {
	isClusterEvent()
}

type MemberJoined struct {
	ID         NodeID
	Name       string
	GossipAddr string
	ServerAddr string
}

func (*MemberJoined) isClusterEvent() {}

type MemberLeft struct {
	ID       NodeID
	SourceID NodeID
}

func (*MemberLeft) isClusterEvent() {}

type MemberUpdated struct {
	ID       NodeID
	SourceID NodeID
	Version  uint64
	Status   Status
}

func (*MemberUpdated) isClusterEvent() {}
