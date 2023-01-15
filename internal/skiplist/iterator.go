package skiplist

// Iterator is an iterator over a Skiplist.
type Iterator[K any, V any] struct {
	next    *listNode[K, V]
	compare Comparator[K]
	level   int
	stopAt  *K
}

func newIterator[K any, V any](
	node *listNode[K, V], level int, comparator Comparator[K], stopAt *K,
) *Iterator[K, V] {
	return &Iterator[K, V]{
		next:    node,
		level:   level,
		compare: comparator,
		stopAt:  stopAt,
	}
}

// HasNext returns true if there are more items in the iterator.
func (it *Iterator[K, V]) HasNext() bool {
	return it.next != nil
}

// Next returns the next key-value pair in the iterator. It panics if there are
// no more items, so HasNext should always be called before calling Next.
func (it *Iterator[K, V]) Next() (key K, value V) {
	if it.next == nil {
		panic("no more items in the iterator")
	}

	node := it.next

	it.next = node.loadNext(it.level)

	if it.stopAt != nil && it.compare(it.next.key, *it.stopAt) > 0 {
		it.next = nil
	}

	return node.key, node.loadValue()
}
