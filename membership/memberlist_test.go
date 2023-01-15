package membership

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/golang/mock/gomock"
	"github.com/maxpoletaev/kiwi/internal/multierror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemberlist_Add(t *testing.T) {
	ctrl := gomock.NewController(t)
	pub := NewMockEventSender(ctrl)

	member1 := Member{ID: 1, Name: "node1"}
	member2 := Member{ID: 2, Name: "node2"}

	gomock.InOrder(
		// New members are broadcasted to the existing members.
		pub.EXPECT().Broadcast(&MemberJoined{ID: 1, Name: "node1"}),
		pub.EXPECT().Broadcast(&MemberJoined{ID: 2, Name: "node2"}),

		// Only then the members should be registered as receivers.
		pub.EXPECT().RegisterReceiver(&member1).Return(nil),
		pub.EXPECT().RegisterReceiver(&member2).Return(nil),
	)

	ml := New(Member{}, log.NewNopLogger(), pub)
	err := ml.Add(member1, member2)

	require.NoError(t, err)
	require.Len(t, ml.Members(), 3)
}

func TestMemberlist_AddFails_BroadcastError(t *testing.T) {
	ctrl := gomock.NewController(t)
	pub := NewMockEventSender(ctrl)
	ml := New(Member{}, log.NewNopLogger(), pub)

	member := Member{ID: 1, Name: "node1"}

	pub.EXPECT().Broadcast(&MemberJoined{ID: 1, Name: "node1"}).Return(assert.AnError)
	err := ml.Add(member)
	require.NotNil(t, err)

	var em *multierror.Error[NodeID]
	require.ErrorAs(t, err, &em)
	require.Equal(t, 1, em.Len())

	err, _ = em.Get(1)
	require.ErrorIs(t, err, assert.AnError)
	require.Len(t, ml.Members(), 1)
}

func TestMemberlist_AddFails_RegisterError(t *testing.T) {
	ctrl := gomock.NewController(t)
	ep := NewMockEventSender(ctrl)
	ml := New(Member{}, log.NewNopLogger(), ep)

	member := Member{ID: 1, Name: "node1"}
	ep.EXPECT().Broadcast(&MemberJoined{ID: 1, Name: "node1"}).Return(nil)
	ep.EXPECT().RegisterReceiver(&member).Return(assert.AnError)

	err := ml.Add(member)
	require.NotNil(t, err)

	var em *multierror.Error[NodeID]
	require.ErrorAs(t, err, &em)
	require.Equal(t, 1, em.Len())

	err, _ = em.Get(1)
	require.ErrorIs(t, err, assert.AnError)
	require.Len(t, ml.Members(), 1)
}

func TestMemberlist_Leave(t *testing.T) {
	ctrl := gomock.NewController(t)

	ep := NewMockEventSender(ctrl)
	ep.EXPECT().Broadcast(&MemberLeft{ID: 1, SourceID: 1}).Return(nil)
	ep.EXPECT().UnregisterReceiver(&Member{ID: 2})

	ml := New(Member{ID: 1}, log.NewNopLogger(), ep)
	ml.members[2] = Member{ID: 2}

	err := ml.Leave()
	require.NoError(t, err)
	require.Len(t, ml.Members(), 1)
}

func TestMemberlist_LeaveFails_BroadcastError(t *testing.T) {
	ctrl := gomock.NewController(t)

	ep := NewMockEventSender(ctrl)
	ep.EXPECT().Broadcast(&MemberLeft{ID: 1, SourceID: 1}).Return(assert.AnError)

	ml := New(Member{ID: 1}, log.NewNopLogger(), ep)

	err := ml.Leave()
	require.ErrorIs(t, err, assert.AnError)
}

func TestMemberlist_SetStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	ep := NewMockEventSender(ctrl)

	ep.EXPECT().Broadcast(&MemberUpdated{
		ID:       2,
		SourceID: 1,
		Version:  2,
		Status:   StatusFaulty,
	})

	ml := New(Member{ID: 1}, log.NewNopLogger(), ep)
	ml.members[2] = Member{ID: 2, Status: StatusHealthy, Version: 1}

	oldStatus, err := ml.SetStatus(2, StatusFaulty)
	require.NoError(t, err)
	require.Equal(t, StatusHealthy, oldStatus)

	m, _ := ml.Member(2)
	require.Equal(t, StatusFaulty, m.Status)
	require.Equal(t, uint64(2), m.Version)
}

func TestMemberlist_SetStatus_NotChanged(t *testing.T) {
	ctrl := gomock.NewController(t)
	ep := NewMockEventSender(ctrl)

	ml := New(Member{ID: 1}, log.NewNopLogger(), ep)
	ml.members[2] = Member{ID: 2, Status: StatusFaulty, Version: 1}

	oldStatus, err := ml.SetStatus(2, StatusFaulty)
	require.NoError(t, err)
	require.Equal(t, StatusFaulty, oldStatus)

	m, _ := ml.Member(2)
	require.Equal(t, StatusFaulty, m.Status)
	require.Equal(t, uint64(1), m.Version)
}

func TestMemberlist_SetStatusFails_MemberNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	ep := NewMockEventSender(ctrl)

	ml := New(Member{ID: 1}, log.NewNopLogger(), ep)
	_, err := ml.SetStatus(2, StatusFaulty)

	require.ErrorIs(t, err, ErrMemberNotFound)
}
