package membership

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestHandleEvent_MemberJoined(t *testing.T) {
	ctrl := gomock.NewController(t)
	ep := NewMockEventPub(ctrl)
	ml := New(Member{}, log.NewNopLogger(), ep)

	ep.EXPECT().RegisterReceiver(&Member{
		ID:         10,
		Name:       "node10",
		ServerAddr: "127.0.0.1:4000",
		GossipAddr: "127.0.0.1:4001",
		Status:     StatusHealthy,
		Version:    1,
	}).Return(nil)

	err := ml.handleEvent(&MemberJoined{
		ID:         10,
		Name:       "node10",
		ServerAddr: "127.0.0.1:4000",
		GossipAddr: "127.0.0.1:4001",
	})

	require.NoError(t, err)
	require.True(t, ml.HasMember(10))
}

func TestHandleEvent_MemberLeft(t *testing.T) {
	ctrl := gomock.NewController(t)
	ep := NewMockEventPub(ctrl)
	ml := New(Member{}, log.NewNopLogger(), ep)

	ml.members[10] = Member{
		ID:         10,
		Name:       "node10",
		ServerAddr: "127.0.0.1:4000",
		GossipAddr: "127.0.0.1:4001",
		Status:     StatusHealthy,
		Version:    1,
	}

	ep.EXPECT().UnregisterReceiver(&Member{
		ID:         10,
		Name:       "node10",
		ServerAddr: "127.0.0.1:4000",
		GossipAddr: "127.0.0.1:4001",
		Status:     StatusHealthy,
		Version:    1,
	})

	err := ml.handleEvent(&MemberLeft{ID: 10})
	require.NoError(t, err)

	require.False(t, ml.HasMember(10))
}
