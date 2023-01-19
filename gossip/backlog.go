package gossip

import (
	"container/list"

	"github.com/maxpoletaev/kiwi/gossip/proto"
)

type Backlog struct {
	maxSize int
	list    *list.List // *proto.Payload
	index   map[uint64]*list.Element
}

func NewBacklog(maxSize int) *Backlog {
	return &Backlog{
		maxSize: maxSize,
		list:    list.New(),
		index:   make(map[uint64]*list.Element),
	}
}

func (bl *Backlog) Add(payload *proto.Payload) {
	if _, ok := bl.index[payload.SeqNumber]; ok {
		return
	}

	el := bl.list.PushFront(payload)
	bl.index[payload.SeqNumber] = el

	if bl.list.Len() > bl.maxSize {
		el := bl.list.Back()
		bl.list.Remove(el)
		pl := el.Value.(*proto.Payload)
		delete(bl.index, pl.SeqNumber)
	}
}

func (bl *Backlog) Get(seq uint64) (*proto.Payload, bool) {
	if el, ok := bl.index[seq]; ok {
		return el.Value.(*proto.Payload), true
	}

	return nil, false
}
