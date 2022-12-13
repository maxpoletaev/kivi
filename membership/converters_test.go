package membership

import (
	"testing"

	"github.com/maxpoletaev/kv/membership/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToStatusProto(t *testing.T) {
	assert.Equal(t, proto.Status_Healthy, ToStatusProto(StatusHealthy))
	assert.Equal(t, proto.Status_Faulty, ToStatusProto(StatusFaulty))
	assert.Panics(t, func() { ToStatusProto(Status(-1)) })
}

func TestToMemberProto(t *testing.T) {
	m := &Member{
		ID:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     StatusHealthy,
	}

	assert.Equal(t, &proto.Member{
		Id:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     proto.Status_Healthy,
	}, ToMemberProto(m))
}

func TestToMembersProto(t *testing.T) {
	members := []Member{
		{
			ID:         1,
			Name:       "node1",
			GossipAddr: "127.0.0.1:4000",
			ServerAddr: "127.0.0.1:4001",
			Version:    1,
			Status:     StatusHealthy,
		},
	}

	result := ToMembersProto(members)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, &proto.Member{
		Id:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     proto.Status_Healthy,
	}, result[0])
}

func TestFromStatusProto(t *testing.T) {
	assert.Equal(t, StatusHealthy, FromStatusProto(proto.Status_Healthy))
	assert.Equal(t, StatusFaulty, FromStatusProto(proto.Status_Faulty))
	assert.Panics(t, func() { FromStatusProto(proto.Status(-1)) })
}

func TestFromMemberProto(t *testing.T) {
	m := &proto.Member{
		Id:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     proto.Status_Healthy,
	}

	assert.Equal(t, Member{
		ID:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     StatusHealthy,
	}, FromMemberProto(m))
}

func TestFromMembersProto(t *testing.T) {
	members := []*proto.Member{
		{
			Id:         1,
			Name:       "node1",
			GossipAddr: "127.0.0.1:4000",
			ServerAddr: "127.0.0.1:4001",
			Version:    1,
			Status:     proto.Status_Healthy,
		},
	}

	result := FromMembersProto(members)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, Member{
		ID:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     StatusHealthy,
	}, result[0])
}

func TestFromEventProto_MemberJoined(t *testing.T) {
	e := &proto.ClusterEvent{
		Event: &proto.ClusterEvent_MemberJoined{
			MemberJoined: &proto.MemberJoinedEvent{
				MemberId:   1,
				MemberName: "node1",
				GossipAddr: "127.0.0.1:4000",
				ServerAddr: "127.0.0.1:4001",
			},
		},
	}

	event, ok := FromEventProto(e).(*MemberJoined)
	require.True(t, ok)

	assert.Equal(t, MemberJoined{
		ID:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
	}, *event)
}

func TestFromEventProto_MemberLeft(t *testing.T) {
	e := &proto.ClusterEvent{
		Event: &proto.ClusterEvent_MemberLeft{
			MemberLeft: &proto.MemberLeftEvent{
				MemberId:       1,
				SourceMemberId: 2,
			},
		},
	}

	event, ok := FromEventProto(e).(*MemberLeft)
	require.True(t, ok)

	assert.Equal(t, MemberLeft{
		ID:       1,
		SourceID: 2,
	}, *event)
}

func TestFromEventProto_MemberUpdated(t *testing.T) {
	e := &proto.ClusterEvent{
		Event: &proto.ClusterEvent_MemberUpdated{
			MemberUpdated: &proto.MemberUpdatedEvent{
				MemberId:       1,
				SourceMemberId: 2,
				Status:         proto.Status_Faulty,
				Version:        1,
			},
		},
	}

	event, ok := FromEventProto(e).(*MemberUpdated)
	require.True(t, ok)

	assert.Equal(t, MemberUpdated{
		ID:       1,
		SourceID: 2,
		Status:   StatusFaulty,
		Version:  1,
	}, *event)
}

func TestToEventProto_MemberJoined(t *testing.T) {
	e := &MemberJoined{
		ID:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
	}

	assert.Equal(t, &proto.ClusterEvent{
		Event: &proto.ClusterEvent_MemberJoined{
			MemberJoined: &proto.MemberJoinedEvent{
				MemberId:   1,
				MemberName: "node1",
				GossipAddr: "127.0.0.1:4000",
				ServerAddr: "127.0.0.1:4001",
			},
		},
	}, ToEventProto(e))
}

func TestToEventProto_MemberLeft(t *testing.T) {
	e := &MemberLeft{
		ID:       1,
		SourceID: 2,
	}

	assert.Equal(t, &proto.ClusterEvent{
		Event: &proto.ClusterEvent_MemberLeft{
			MemberLeft: &proto.MemberLeftEvent{
				MemberId:       1,
				SourceMemberId: 2,
			},
		},
	}, ToEventProto(e))
}

func TestToEventProto_MemberUpdated(t *testing.T) {
	e := &MemberUpdated{
		ID:       1,
		SourceID: 2,
		Version:  1,
		Status:   StatusFaulty,
	}

	assert.Equal(t, &proto.ClusterEvent{
		Event: &proto.ClusterEvent_MemberUpdated{
			MemberUpdated: &proto.MemberUpdatedEvent{
				MemberId:       1,
				SourceMemberId: 2,
				Version:        1,
				Status:         proto.Status_Faulty,
			},
		},
	}, ToEventProto(e))
}
