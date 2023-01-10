package membership

import (
	"testing"

	"github.com/maxpoletaev/kv/membership/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToProtoStatus(t *testing.T) {
	assert.Equal(t, proto.Status_Healthy, ToProtoStatus(StatusHealthy))
	assert.Equal(t, proto.Status_Faulty, ToProtoStatus(StatusFaulty))
	assert.Panics(t, func() { ToProtoStatus(Status(-1)) })
}

func TestToProtoMember(t *testing.T) {
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
	}, ToProtoMember(m))
}

func TestToProtoMembers(t *testing.T) {
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

	result := ToProtoMembers(members)
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

func TestFromProtoStatus(t *testing.T) {
	assert.Equal(t, StatusHealthy, FromProtoStatus(proto.Status_Healthy))
	assert.Equal(t, StatusFaulty, FromProtoStatus(proto.Status_Faulty))
	assert.Panics(t, func() { FromProtoStatus(proto.Status(-1)) })
}

func TestFromProtoMember(t *testing.T) {
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
	}, FromProtoMember(m))
}

func TestFromProtoMembers(t *testing.T) {
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

	result := FromProtoMembers(members)
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

func TestFromProtoEvent_MemberJoined(t *testing.T) {
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

	event, ok := FromProtoEvent(e).(*MemberJoined)
	require.True(t, ok)

	assert.Equal(t, MemberJoined{
		ID:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
	}, *event)
}

func TestFromProtoEvent_MemberLeft(t *testing.T) {
	e := &proto.ClusterEvent{
		Event: &proto.ClusterEvent_MemberLeft{
			MemberLeft: &proto.MemberLeftEvent{
				MemberId:       1,
				SourceMemberId: 2,
			},
		},
	}

	event, ok := FromProtoEvent(e).(*MemberLeft)
	require.True(t, ok)

	assert.Equal(t, MemberLeft{
		ID:       1,
		SourceID: 2,
	}, *event)
}

func TestFromProtoEvent_MemberUpdated(t *testing.T) {
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

	event, ok := FromProtoEvent(e).(*MemberUpdated)
	require.True(t, ok)

	assert.Equal(t, MemberUpdated{
		ID:       1,
		SourceID: 2,
		Status:   StatusFaulty,
		Version:  1,
	}, *event)
}

func TestToProtoEvent_MemberJoined(t *testing.T) {
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
	}, ToProtoEvent(e))
}

func TestToProtoEvent_MemberLeft(t *testing.T) {
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
	}, ToProtoEvent(e))
}

func TestToProtoEvent_MemberUpdated(t *testing.T) {
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
	}, ToProtoEvent(e))
}
