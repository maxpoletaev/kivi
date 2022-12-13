package broadcast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
)

func TestGossipDelegate_Deliver(t *testing.T) {
	delegate := NewGossipEventDelegate()
	defer delegate.Close()

	b, err := protobuf.Marshal(&proto.ClusterEvent{
		Event: &proto.ClusterEvent_MemberJoined{
			MemberJoined: &proto.MemberJoinedEvent{
				MemberId:   1,
				MemberName: "node1",
				ServerAddr: "127.0.0.1:3000",
				GossipAddr: "127.0.0.1:3001",
			},
		},
	})
	require.Nil(t, err)

	done := make(chan struct{})

	var event membership.ClusterEvent

	go func() {
		event = <-delegate.Events()
		close(done)
	}()

	err = delegate.Deliver(b)
	assert.Nil(t, err)
	<-done

	assert.Len(t, delegate.Events(), 0)
	assert.Equal(t, &membership.MemberJoined{
		ID:         1,
		Name:       "node1",
		ServerAddr: "127.0.0.1:3000",
		GossipAddr: "127.0.0.1:3001",
	}, event)
}

func TestGossipDeletage_Deliver_UnmarshalFails(t *testing.T) {
	delegate := NewGossipEventDelegate()
	defer delegate.Close()

	err := delegate.Deliver([]byte("InvalidProtobufContents"))
	assert.ErrorContains(t, err, "failed to unmarshal cluster event")
}
