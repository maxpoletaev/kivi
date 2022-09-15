package queue

import (
	"sync"

	"github.com/maxpoletaev/kv/gossip/proto"
	"github.com/maxpoletaev/kv/internal/heap"
)

// OrderedQueue is a message queue that guarantees delivery order.
// It is safe to read and write from multiple threads concurrently.
type OrderedQueue struct {
	mut       sync.RWMutex
	pq        *heap.Heap[*proto.GossipMessage]
	waiting   map[uint64]bool
	delivered uint64
}

func New() *OrderedQueue {
	q := &OrderedQueue{
		waiting: make(map[uint64]bool),
		pq: heap.New(func(a, b *proto.GossipMessage) bool {
			return a.SeqNumber < b.SeqNumber // min heap
		}),
	}

	return q
}

// Len returns the current number of queued messages. A non-zero result
// does not mean that a is ready to be poped from the queue.
func (q *OrderedQueue) Len() int {
	q.mut.RLock()
	defer q.mut.RUnlock()

	return len(q.waiting)
}

// Push adds a new message into the queue. Messages can be pushed in any order.
// However, messages that are already in the queue or have been already delivered
// will not be added.
func (q *OrderedQueue) Push(msg *proto.GossipMessage) bool {
	q.mut.Lock()
	defer q.mut.Unlock()

	if q.delivered >= msg.SeqNumber || q.waiting[msg.SeqNumber] {
		return false
	}

	q.waiting[msg.SeqNumber] = true

	q.pq.Push(msg)

	return true
}

// PopNext returns message wich directly succeeds the last poped message.
// This will return nil if there is no successor, even if the queue is not empty.
func (q *OrderedQueue) PopNext() *proto.GossipMessage {
	q.mut.Lock()
	defer q.mut.Unlock()

	if q.pq.Len() == 0 {
		return nil
	}

	msg := q.pq.Peek()

	nextSeqNumber := q.delivered + 1

	if q.delivered == 0 || msg.SeqNumber == nextSeqNumber {
		delete(q.waiting, msg.SeqNumber)
		q.delivered = msg.SeqNumber
		return q.pq.Pop()
	}

	return nil
}
