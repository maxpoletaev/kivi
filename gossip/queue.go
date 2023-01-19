package gossip

import (
	"math"
	"sort"
	"sync"

	"github.com/maxpoletaev/kiwi/gossip/proto"
	"github.com/maxpoletaev/kiwi/internal/generic"
	"github.com/maxpoletaev/kiwi/internal/heap"
)

// MessageQueue is a message queue that guarantees delivery order.
// It is safe to read and write from multiple threads concurrently.
type MessageQueue struct {
	mut       sync.RWMutex
	pq        *heap.Heap[*proto.Payload]
	waiting   map[uint64]struct{}
	delivered uint64
	rollover  bool
	started   bool
}

func NewQueue() *MessageQueue {
	q := &MessageQueue{
		waiting: make(map[uint64]struct{}),
		pq: heap.New(func(a, b *proto.Payload) bool {
			wrapped := a.SeqRollover != b.SeqRollover
			return (!wrapped && a.SeqNumber < b.SeqNumber) || (wrapped && a.SeqNumber > b.SeqNumber)
		}),
	}

	return q
}

// Len returns the current number of currently buffered messages. A non-zero result
// does not mean that there is a message that is ready to be poped from the queue.
func (q *MessageQueue) Len() int {
	q.mut.RLock()
	defer q.mut.RUnlock()

	return len(q.waiting)
}

// isDeliverable returns true if the message is ready to be delivered/removed from the queue.
func (q *MessageQueue) isDeliverable(pl *proto.Payload) bool {
	if !q.started {
		return true
	}

	if q.rollover == pl.SeqRollover {
		return q.delivered+1 == pl.SeqNumber
	}

	return q.delivered == math.MaxUint64 && pl.SeqNumber == 0
}

// wasDelivered returns true if the message was already delivered.
func (q *MessageQueue) wasDelivered(pl *proto.Payload) bool {
	if !q.started {
		return false
	}

	if q.rollover == pl.SeqRollover {
		return q.delivered >= pl.SeqNumber
	}

	return q.delivered <= pl.SeqNumber
}

// Push adds a new message into the queue. Messages can be pushed in any order.
// However, messages that are already in the queue or have been already delivered
// will not be added.
func (q *MessageQueue) Push(pl *proto.Payload) bool {
	q.mut.Lock()
	defer q.mut.Unlock()

	// Skip messages that are already in the queue or have been already delivered.
	if _, queued := q.waiting[pl.SeqNumber]; queued || q.wasDelivered(pl) {
		return false
	}

	q.waiting[pl.SeqNumber] = struct{}{}

	q.pq.Push(pl)

	return true
}

// NextSeqNum returns the next sequence number that is expected to be received.
func (q *MessageQueue) NextSeqNum() uint64 {
	q.mut.RLock()
	defer q.mut.RUnlock()

	if !q.started {
		return 0
	}

	return q.delivered + 1
}

// PopNext returns the next message in the queue. If the next message is not
// available, it returns nil, even if there are other messages in the queue.
// The first call to PopNext will return the message with the lowest sequence number
// that is currently in the queue. Subsequent calls will return the next message
// in the sequence.
func (q *MessageQueue) PopNext() *proto.Payload {
	q.mut.Lock()
	defer q.mut.Unlock()

	var pl *proto.Payload

	for {
		if q.pq.Len() == 0 {
			return nil
		}

		pl = q.pq.Peek()

		// The top message is not in the list of waiting messages, which means that the message
		// was skipped by the SkipTo method. We can safely remove it from the queue.
		if _, waiting := q.waiting[pl.SeqNumber]; !waiting {
			q.pq.Pop()
			continue
		}

		break
	}

	if !q.isDeliverable(pl) {
		return nil
	}

	delete(q.waiting, pl.SeqNumber)

	q.rollover = pl.SeqRollover
	q.delivered = pl.SeqNumber
	q.started = true

	return q.pq.Pop()
}

// SkipTo marks the message with the given sequence number as delivered, even if it was
// not received. This is useful when we want to unblock the queue, but we don't want to
// receive the message.
func (q *MessageQueue) SkipTo(seq uint64) {
	q.mut.Lock()
	defer q.mut.Unlock()

	for i := q.delivered; i != seq; i++ {
		delete(q.waiting, i)
	}

	if seq < q.delivered {
		q.rollover = !q.rollover
	}

	q.delivered = seq
}

// FindGaps returns a list of sequence numbers that are missing from the queue,
// which successors are currently in the queue. This may be used to request
// missing messages from other nodes. The complexity of this operation is O(n log n)
// due to the sorting of the sequence numbers, so it should be used sparingly.
func (q *MessageQueue) FindGaps() []uint64 {
	q.mut.RLock()
	defer q.mut.RUnlock()

	waiting := generic.MapKeys(q.waiting)
	sort.Slice(waiting, func(i, j int) bool {
		return waiting[i] < waiting[j]
	})

	var missing []uint64

	lastSeqNum := q.delivered

	for _, seq := range waiting {
		if seq != lastSeqNum+1 {
			for i := lastSeqNum + 1; i < seq; i++ {
				missing = append(missing, i)
			}
		}

		lastSeqNum = seq
	}

	return missing
}
