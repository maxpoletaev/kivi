package queue

import (
	"math"
	"sync"

	"github.com/maxpoletaev/kv/gossip/proto"
	"github.com/maxpoletaev/kv/internal/heap"
)

// OrderedQueue is a message queue that guarantees delivery order.
// It is safe to read and write from multiple threads concurrently.
type OrderedQueue struct {
	mut       sync.RWMutex
	pq        *heap.Heap[*proto.GossipMessage]
	waiting   map[uint64]struct{}
	delivered uint64
	rollover  bool
	started   bool
}

func New() *OrderedQueue {
	q := &OrderedQueue{
		waiting: make(map[uint64]struct{}),
		pq: heap.New(func(a, b *proto.GossipMessage) bool {
			wrapped := a.SeqRollover != b.SeqRollover
			return (!wrapped && a.SeqNumber < b.SeqNumber) || (wrapped && a.SeqNumber > b.SeqNumber)
		}),
	}

	return q
}

// Len returns the current number of currently buffered messages. A non-zero result
// does not mean that there is a message that is ready to be poped from the queue.
func (q *OrderedQueue) Len() int {
	q.mut.RLock()
	defer q.mut.RUnlock()

	return len(q.waiting)
}

// isDeliverable returns true if the message is ready to be delivered/removed from the queue.
func (q *OrderedQueue) isDeliverable(msg *proto.GossipMessage) bool {
	if !q.started {
		return true
	}

	if q.rollover == msg.SeqRollover {
		return q.delivered+1 == msg.SeqNumber
	}

	return q.delivered == math.MaxUint64 && msg.SeqNumber == 0
}

// beenDelivered returns true if the message has been already delivered.
func (q *OrderedQueue) beenDelivered(msg *proto.GossipMessage) bool {
	if !q.started {
		return false
	}

	if q.rollover == msg.SeqRollover {
		return q.delivered >= msg.SeqNumber
	}

	return q.delivered <= msg.SeqNumber
}

// Push adds a new message into the queue. Messages can be pushed in any order.
// However, messages that are already in the queue or have been already delivered
// will not be added.
func (q *OrderedQueue) Push(msg *proto.GossipMessage) bool {
	q.mut.Lock()
	defer q.mut.Unlock()

	// Skip messages that are already in the queue or have been already delivered.
	if _, queued := q.waiting[msg.SeqNumber]; queued || q.beenDelivered(msg) {
		return false
	}

	q.waiting[msg.SeqNumber] = struct{}{}

	q.pq.Push(msg)

	return true
}

// PopNext returns the next message in the queue. If the next message is not
// available, it returns nil, even if there are other messages in the queue.
// The first call to PopNext will return the message with the lowest sequence number
// that is currently in the queue. Subsequent calls will return the next message
// in the sequence.
func (q *OrderedQueue) PopNext() *proto.GossipMessage {
	q.mut.Lock()
	defer q.mut.Unlock()

	if q.pq.Len() == 0 {
		return nil
	}

	msg := q.pq.Peek()
	if !q.isDeliverable(msg) {
		return nil
	}

	delete(q.waiting, msg.SeqNumber)

	q.rollover = msg.SeqRollover

	q.delivered = msg.SeqNumber

	q.started = true

	return q.pq.Pop()
}
