package skiplist

import (
	"errors"
	"fmt"
	"math/rand"
)

const (
	maxHeight    = 12
	branchFactor = 4
)

var ErrNotFound = errors.New("not found")

type Comparator[K comparable] func(a, b K) int

type listNodes[K comparable, V any] [maxHeight]*listNode[K, V]

type listNode[K comparable, V any] struct {
	key   K
	value V
	next  listNodes[K, V]
}

type Skiplist[K comparable, V any] struct {
	head    *listNode[K, V]
	compare Comparator[K]
	height  int
	size    int
}

func New[K comparable, V any](comparator Comparator[K]) *Skiplist[K, V] {
	head := &listNode[K, V]{}

	return &Skiplist[K, V]{
		compare: comparator,
		head:    head,
	}
}

func (l *Skiplist[K, V]) Height() int {
	return l.height
}

func (l *Skiplist[K, V]) Size() int {
	return l.size
}

func (l *Skiplist[K, V]) findGreaterOrEqual(key K, searchPath *listNodes[K, V]) *listNode[K, V] {
	if l.height == 0 {
		return nil
	}

	level := l.height - 1
	node := l.head

	for {
		next := node.next[level]

		if next != nil && l.compare(key, next.key) >= 0 {
			node = next
			continue
		}

		if searchPath != nil {
			searchPath[level] = node
		}

		if level == 0 {
			break
		}

		level--
	}

	if node == l.head || l.compare(key, node.key) > 0 {
		node = node.next[0]
	}

	return node
}

func (l *Skiplist[K, V]) Insert(key K, value V) {
	var searchPath listNodes[K, V]

	node := l.findGreaterOrEqual(key, &searchPath)

	if node != nil && l.compare(key, node.key) == 0 {
		node.value = value
		return
	}

	newNode := &listNode[K, V]{key: key, value: value}
	newHeight := randomHeight()

	if newHeight > l.height {
		for i := l.height; i < newHeight; i++ {
			searchPath[i] = l.head
		}

		l.height = newHeight
	}

	for i := 0; i < newHeight; i++ {
		newNode.next[i] = searchPath[i].next[i]
		searchPath[i].next[i] = newNode
	}

	l.size++
}

func (l *Skiplist[K, V]) Scan() *Iterator[K, V] {
	return newIterator(l.head.next[0], 0, l.compare, nil)
}

func (l *Skiplist[K, V]) ScanFrom(key K) *Iterator[K, V] {
	node := l.findGreaterOrEqual(key, nil)
	return newIterator(node, 0, l.compare, nil)
}

func (l *Skiplist[K, V]) ScanRange(start, end K) *Iterator[K, V] {
	node := l.findGreaterOrEqual(start, nil)
	return newIterator(node, 0, l.compare, &end)
}

func (l *Skiplist[K, V]) Get(key K) (ret V, err error) {
	node := l.findGreaterOrEqual(key, nil)

	if node == nil || l.compare(key, node.key) != 0 {
		return ret, ErrNotFound
	}

	return node.value, nil
}

func randomHeight() int {
	height := 1

	for height < maxHeight && ((rand.Int() % branchFactor) == 0) {
		height++
	}

	return height
}

// construct is a helper to simplify skiplist construction during tests.
func construct[K comparable, V any](keys [][]K, keyComparator Comparator[K], defaultValue V) *Skiplist[K, V] {
	nodes := make(map[K]*listNode[K, V], 0)
	head := &listNode[K, V]{}
	size := 0

	if len(keys) > 0 {
		size = len(keys[0])

		for _, key := range keys[0] {
			nodes[key] = &listNode[K, V]{key: key, value: defaultValue}
		}
	}

	for level := 0; level < len(keys); level++ {
		tail := head.next[level]

		for _, key := range keys[level] {
			node := nodes[key]
			if node == nil {
				panic(fmt.Sprintf("invalid list: node %v is defined at level 0 but missing from level %d", key, level))
			}

			if tail == nil {
				head.next[level] = node
				tail = node
			} else {
				tail.next[level] = node
				tail = node
			}
		}
	}

	return &Skiplist[K, V]{
		head:    head,
		size:    size,
		height:  len(keys),
		compare: keyComparator,
	}
}
