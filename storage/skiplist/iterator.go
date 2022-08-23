package skiplist

type Iterator[K comparable, V any] struct {
	next    *listNode[K, V]
	compare Comparator[K]
	level   int
	stopAt  *K
}

func newIterator[K comparable, V any](node *listNode[K, V], level int,
	comparator Comparator[K], stopAt *K) *Iterator[K, V] {

	return &Iterator[K, V]{
		next:    node,
		level:   level,
		compare: comparator,
		stopAt:  stopAt,
	}
}

func (it *Iterator[K, V]) HasNext() bool {
	return it.next != nil
}

func (it *Iterator[K, V]) Next() (key K, value V) {
	if it.next == nil {
		panic("no more items in the iterator")
	}

	node := it.next
	next := node.next[it.level]

	it.next = next

	if it.stopAt != nil && it.compare(next.key, *it.stopAt) > 0 {
		it.next = nil
	}

	return node.key, node.value
}
