package skiplist

import (
	"sync/atomic"
	"unsafe"
)

type listNodes[K comparable, V any] [maxHeight]*listNode[K, V]

type listNode[K comparable, V any] struct {
	key    K
	value  atomic.Value
	next   listNodes[K, V]
	marked int32
}

func (n *listNode[K, V]) casNext(level int, oldNext, newNext *listNode[K, V]) bool {
	nextPtr := (*unsafe.Pointer)(unsafe.Pointer(&n.next[level]))
	return atomic.CompareAndSwapPointer(nextPtr, unsafe.Pointer(oldNext), unsafe.Pointer(newNext))
}

func (n *listNode[K, V]) storeNext(level int, next *listNode[K, V]) {
	nextPtr := (*unsafe.Pointer)(unsafe.Pointer(&n.next[level]))
	atomic.StorePointer(nextPtr, unsafe.Pointer(next))
}

func (n *listNode[K, V]) loadNext(level int) *listNode[K, V] {
	node := n

	for {
		nextPtr := (*unsafe.Pointer)(unsafe.Pointer(&node.next[level]))
		next := (*listNode[K, V])(atomic.LoadPointer(nextPtr))

		if next == nil || !next.isMarked() {
			return next
		}

		// Since next is marked, try to remove it from the list.
		node.casNext(level, next, next.loadNext(level))

		node = next
	}
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

func (n *listNode[K, V]) setMarked() {
	atomic.StoreInt32(&n.marked, 1)
}

func (n *listNode[K, V]) isMarked() bool {
	return atomic.LoadInt32(&n.marked) == 1
}
