package broadcast

import (
	"testing"

	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	event, ok := fromProtoEvent(e).(*membership.MemberJoined)
	require.True(t, ok)

	assert.Equal(t, membership.MemberJoined{
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

	event, ok := fromProtoEvent(e).(*membership.MemberLeft)
	require.True(t, ok)

	assert.Equal(t, membership.MemberLeft{
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

	event, ok := fromProtoEvent(e).(*membership.MemberUpdated)
	require.True(t, ok)

	assert.Equal(t, membership.MemberUpdated{
		ID:       1,
		SourceID: 2,
		Status:   membership.StatusFaulty,
		Version:  1,
	}, *event)
}

func TestToProtoEvent_MemberJoined(t *testing.T) {
	e := &membership.MemberJoined{
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
	}, toProtoEvent(e))
}

func TestToProtoEvent_MemberLeft(t *testing.T) {
	e := &membership.MemberLeft{
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
	}, toProtoEvent(e))
}

func TestToProtoEvent_MemberUpdated(t *testing.T) {
	e := &membership.MemberUpdated{
		ID:       1,
		SourceID: 2,
		Version:  1,
		Status:   membership.StatusFaulty,
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
	}, toProtoEvent(e))
}
