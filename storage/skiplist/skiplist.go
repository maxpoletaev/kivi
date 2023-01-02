package skiplist

import (
	"math/rand"
	"sync/atomic"
)

const (
	maxHeight    = 12
	branchFactor = 4
)

// Comparator is a function that compares two keys.
// It returns a negative number if a < b, 0 if a == b, and a positive number if a > b.
type Comparator[K comparable] func(a, b K) int

// Skiplist is a generic skiplist implementation. It is lock-free and supports
// mutiple concurrent readers and multiple concurrent writers at the same time.
type Skiplist[K comparable, V any] struct {
	head        *listNode[K, V]
	compareKeys Comparator[K]
	height      int32
	size        int32
}

// New returns a new Skiplist. The comparator is used to compare keys.
func New[K comparable, V any](comparator Comparator[K]) *Skiplist[K, V] {
	head := &listNode[K, V]{}

	return &Skiplist[K, V]{
		compareKeys: comparator,
		head:        head,
	}
}

func (l *Skiplist[K, V]) loadHeight() int {
	return int(atomic.LoadInt32(&l.height))
}

func (l *Skiplist[K, V]) casHeigth(oldHeight, newHeight int) bool {
	return atomic.CompareAndSwapInt32(&l.height, int32(oldHeight), int32(newHeight))
}

// Height returns the height of the list.
func (l *Skiplist[K, V]) Height() int {
	return l.loadHeight()
}

// Size returns the number of key-value pairs in the list.
func (l *Skiplist[K, V]) Size() int {
	return int(atomic.LoadInt32(&l.size))
}

func (l *Skiplist[K, V]) findLess(key K, searchPath *listNodes[K, V]) *listNode[K, V] {
	height := l.loadHeight()
	if height == 0 {
		return nil
	}

	level := height - 1
	node := l.head

	for {
		next := node.loadNext(level)

		if next != nil && l.compareKeys(key, next.key) > 0 {
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

	return node
}

// Insert inserts a new key-value pair into the list. If the key already exists, the value is updated.
func (l *Skiplist[K, V]) Insert(key K, value V) {
	height := l.Height()
	newheight := randomHeight()

	if newheight > height {
		if !l.casHeigth(height, newheight) {
			newheight = height
		}
	}

	newnode := &listNode[K, V]{key: key}
	newnode.storeValue(value)

loop:
	for {
		var searchPath listNodes[K, V]

		l.findLess(key, &searchPath)

		for level := height; level < newheight; level++ {
			searchPath[level] = l.head
		}

		node := searchPath[0].loadNext(0)

		// If the key already exists, just update the value.
		if node != nil && l.compareKeys(key, node.key) == 0 {
			node.storeValue(value)
			return
		}

		for level := 0; level < newheight; level++ {
			next := searchPath[level].loadNext(level)
			newnode.storeNext(level, next)
		}

		prev := searchPath[0]
		next := prev.loadNext(0)

		// Insert to the base level first. If it fails, we need to start over.
		if !prev.casNext(0, next, newnode) {
			continue loop
		}

		// Update the upper levels. Repeat until we succeed.
		for level := 1; level < newheight; level++ {
			for {
				prev := searchPath[level]
				next := prev.loadNext(level)

				// Update the next pointer if it's still the same as when we
				// started. Otherwise, search again for the insertion point.
				if !prev.casNext(level, next, newnode) {
					l.findLess(key, &searchPath)
					continue
				}

				break
			}
		}

		break
	}

	atomic.AddInt32(&l.size, 1)
}

// Remove removes the key-value pair with the given key from the list.
// It returns true if the key was found. Note that the node is not physically
// removed from the list. Instead, it is marked as deleted and will be removed
// once loadNext() is called on the previous node, which happens during findLess() call.
func (l *Skiplist[K, V]) Remove(key K) bool {
	var searchPath listNodes[K, V]

	l.findLess(key, &searchPath)

	prev := searchPath[0]
	if prev == nil {
		return false
	}

	node := prev.loadNext(0)
	if node == nil || l.compareKeys(key, node.key) != 0 {
		return false
	}

	node.setMarked()

	if atomic.AddInt32(&l.size, -1) < 0 {
		panic("skiplist: negative size")
	}

	l.findLess(key, nil) // triggers marked node cleanup

	return true
}

// Scan returns an iterator that scans the list from the beginning.
// Note that the list may change while the iterator is in use.
func (l *Skiplist[K, V]) Scan() *Iterator[K, V] {
	return newIterator(l.head.loadNext(0), 0, l.compareKeys, nil)
}

// ScanFrom returns an iterator that scans the list from the given key.
// Note that the list may change while the iterator is in use.
func (l *Skiplist[K, V]) ScanFrom(key K) *Iterator[K, V] {
	var node *listNode[K, V]
	if prev := l.findLess(key, nil); prev != nil {
		node = prev.loadNext(0)
	}

	return newIterator(node, 0, l.compareKeys, nil)
}

// ScanRange returns an iterator that scans the list from the given start key to the given end key.
// Note that the list may change while the iterator is in use.
func (l *Skiplist[K, V]) ScanRange(start, end K) *Iterator[K, V] {
	var node *listNode[K, V]
	if prev := l.findLess(start, nil); prev != nil {
		node = prev.loadNext(0)
	}

	return newIterator(node, 0, l.compareKeys, &end)
}

// Contains returns true if the list contains the given key.
func (l *Skiplist[K, V]) Contains(key K) bool {
	var node *listNode[K, V]

	if prev := l.findLess(key, nil); prev != nil {
		node = prev.loadNext(0)
	}

	if node == nil || l.compareKeys(key, node.key) != 0 {
		return false
	}

	return true
}

// Get returns the value for the given key. If the key is not found, ErrNotFound is returned.
func (l *Skiplist[K, V]) Get(key K) (ret V, found bool) {
	var node *listNode[K, V]

	if prev := l.findLess(key, nil); prev != nil {
		node = prev.loadNext(0)
	}

	if node == nil || l.compareKeys(key, node.key) != 0 {
		return ret, false
	}

	return node.loadValue(), true
}

// GetLess returns the value for the key that is less than the given key.
func (l *Skiplist[K, V]) LessOrEqual(key K) (retk K, retv V, found bool) {
	node := l.findLess(key, nil)
	if node == nil {
		return retk, retv, false
	}

	for {
		next := node.loadNext(0)

		if next != nil && l.compareKeys(key, next.key) >= 0 {
			node = next
			continue
		}

		break
	}

	if node == l.head {
		return retk, retv, false
	}

	return node.key, node.loadValue(), true
}

func randomHeight() int {
	height := 1

	for height < maxHeight && ((rand.Int() % branchFactor) == 0) {
		height++
	}

	return height
}
