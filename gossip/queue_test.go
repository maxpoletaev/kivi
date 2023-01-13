package gossip

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kiwi/gossip/proto"
)

func TestQueue(t *testing.T) {
	type test struct {
		prepareFunc func(q *MessageQueue)
		assertFunc  func(t *testing.T, q *MessageQueue)
	}

	tests := map[string]test{
		"PushToAnEmptyQueue": {
			prepareFunc: func(q *MessageQueue) {},
			assertFunc: func(t *testing.T, q *MessageQueue) {
				msg := &proto.GossipMessage{SeqNumber: 100}
				pushed := q.Push(msg)

				assert.Equal(t, 1, q.Len())
				assert.True(t, pushed)
			},
		},
		"PushWhenMessageAlreadyReceived": {
			prepareFunc: func(q *MessageQueue) {
				q.Push(&proto.GossipMessage{SeqNumber: 100})
			},
			assertFunc: func(t *testing.T, q *MessageQueue) {
				pushed := q.Push(&proto.GossipMessage{SeqNumber: 100})

				assert.Equal(t, 1, q.Len())
				assert.False(t, pushed)
			},
		},
		"PushWhenMessageAlreadyDelivered": {
			prepareFunc: func(q *MessageQueue) {
				q.Push(&proto.GossipMessage{SeqNumber: 100})
				q.PopNext()
			},
			assertFunc: func(t *testing.T, q *MessageQueue) {
				pushed := q.Push(&proto.GossipMessage{SeqNumber: 100})

				assert.Equal(t, 0, q.Len())
				assert.False(t, pushed)
			},
		},
		"PushWhenNewerMessageAlreadyDelivered": {
			prepareFunc: func(q *MessageQueue) {
				q.Push(&proto.GossipMessage{SeqNumber: 100})
				q.PopNext()
			},
			assertFunc: func(t *testing.T, q *MessageQueue) {
				pushed := q.Push(&proto.GossipMessage{SeqNumber: 99})

				assert.Equal(t, 0, q.Len())
				assert.False(t, pushed)
			},
		},
		"PopAfterUnorderedPush": {
			prepareFunc: func(q *MessageQueue) {
				q.Push(&proto.GossipMessage{SeqNumber: 2})
				q.Push(&proto.GossipMessage{SeqNumber: 3})
				q.Push(&proto.GossipMessage{SeqNumber: 1})
				q.Push(&proto.GossipMessage{SeqNumber: 3})
				q.Push(&proto.GossipMessage{SeqNumber: 2})
				q.Push(&proto.GossipMessage{SeqNumber: 2})
				q.Push(&proto.GossipMessage{SeqNumber: 1})
				q.Push(&proto.GossipMessage{SeqNumber: 2})
			},
			assertFunc: func(t *testing.T, q *MessageQueue) {
				require.Equal(t, 3, q.Len())

				msg1 := q.PopNext()
				msg2 := q.PopNext()
				msg3 := q.PopNext()

				require.NotNil(t, msg1)
				require.NotNil(t, msg2)
				require.NotNil(t, msg3)

				assert.Equal(t, uint64(1), msg1.SeqNumber)
				assert.Equal(t, uint64(2), msg2.SeqNumber)
				assert.Equal(t, uint64(3), msg3.SeqNumber)
			},
		},
		"PopFromEmptyQueue": {
			prepareFunc: func(q *MessageQueue) {},
			assertFunc: func(t *testing.T, q *MessageQueue) {
				msg := q.PopNext()

				assert.Nil(t, msg)
			},
		},
		"PopPreviousMessageNotReceived": {
			prepareFunc: func(q *MessageQueue) {
				q.Push(&proto.GossipMessage{SeqNumber: 1})
				q.Push(&proto.GossipMessage{SeqNumber: 3})
			},
			assertFunc: func(t *testing.T, q *MessageQueue) {
				msg1 := q.PopNext()
				msg2 := q.PopNext()

				assert.NotNil(t, msg1)
				assert.Nil(t, msg2)

				assert.Equal(t, uint64(1), msg1.SeqNumber)
			},
		},
		"PushPopWithOverflow": {
			prepareFunc: func(q *MessageQueue) {
				q.Push(&proto.GossipMessage{SeqNumber: math.MaxUint64})
				q.Push(&proto.GossipMessage{SeqNumber: 1, SeqRollover: true})
				q.Push(&proto.GossipMessage{SeqNumber: 0, SeqRollover: true})
			},
			assertFunc: func(t *testing.T, q *MessageQueue) {
				require.Equal(t, 3, q.Len())

				msg1 := q.PopNext()
				require.NotNil(t, msg1)
				require.Equal(t, uint64(math.MaxUint64), msg1.SeqNumber)

				msg2 := q.PopNext()
				require.NotNil(t, msg2)
				require.Equal(t, uint64(0), msg2.SeqNumber)

				msg3 := q.PopNext()
				require.NotNil(t, msg3)
				require.Equal(t, uint64(1), msg3.SeqNumber)
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			q := NewQueue()
			tt.prepareFunc(q)
			tt.assertFunc(t, q)
		})
	}
}
