package queue

import (
	"sync"

	"github.com/maxpoletaev/kv/gossip/proto"
	"github.com/maxpoletaev/kv/internal/heap"
)

type Option func(q *OrderedQueue)

type OrderedQueue struct {
	mut       sync.RWMutex
	pq        *heap.Heap[*proto.GossipMessage]
	waiting   map[uint64]bool
	delivered uint64
}

func New(opts ...Option) *OrderedQueue {
	q := &OrderedQueue{
		waiting: make(map[uint64]bool),
		pq: heap.New(func(a, b *proto.GossipMessage) bool {
			return a.SeqNumber < b.SeqNumber // min heap
		}),
	}

	for _, opt := range opts {
		opt(q)
	}

	return q
}

func (q *OrderedQueue) Len() int {
	q.mut.RLock()
	defer q.mut.RUnlock()

	return len(q.waiting)
}

// Push adds a new message into the queue and provide
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
