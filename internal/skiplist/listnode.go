package skiplist

import (
	"sync/atomic"
	"unsafe"
)

type listNodes[K any, V any] [maxHeight]*listNode[K, V]

type listNode[K any, V any] struct {
	key   K
	value atomic.Value
	next  [maxHeight]unsafe.Pointer
}

func (n *listNode[K, V]) storeNext(level int, next *listNode[K, V]) {
	atomic.StorePointer(&n.next[level], unsafe.Pointer(next))
}

func (n *listNode[K, V]) loadNext(level int) *listNode[K, V] {
	return (*listNode[K, V])(atomic.LoadPointer(&n.next[level]))
}

func (n *listNode[K, V]) storeValue(value V) {
	n.value.Store(value)
}

func (n *listNode[K, V]) loadValue() (ret V) {
	if val := n.value.Load(); val != nil {
		return val.(V)
	}

	return ret
}
