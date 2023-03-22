package heap

import "golang.org/x/exp/constraints"

type comparator[T any] func(a, b T) bool

// Min is a comparator function to implement min-heap for any type that supports ordering.
func Min[T constraints.Ordered](a, b T) bool { return a < b }

// Max is a comparator function to implement max-heap for any type that supports ordering.
func Max[T constraints.Ordered](a, b T) bool { return a > b }

// Heap is a generic port of container.Heap. Not safe to use concurrently.
type Heap[T any] struct {
	less  comparator[T]
	items []T
}

// New creates new heap data structure with given comparator.
func New[T any](less comparator[T]) *Heap[T] {
	return &Heap[T]{
		items: make([]T, 0),
		less:  less,
	}
}

func (h *Heap[T]) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *Heap[T]) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.less(h.items[j], h.items[i]) {
			break
		}

		h.swap(i, j)

		j = i
	}
}

func (h *Heap[T]) down(i0, n int) bool {
	i := i0

	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int rolling
			break
		}

		j := j1 // left child

		if j2 := j1 + 1; j2 < n && h.less(h.items[j2], h.items[j1]) {
			j = j2 // = 2*i + 2  // right child
		}

		if !h.less(h.items[j], h.items[i]) {
			break
		}

		h.swap(i, j)

		i = j
	}

	return i > i0
}

// Len returns current number of elements on the structure.
func (h *Heap[T]) Len() int {
	return len(h.items)
}

// Push adds new element to the heap in O(log n) time.
func (h *Heap[T]) Push(val T) {
	h.items = append(h.items, val)
	h.up(h.Len() - 1)
}

// Pop returns and removes the top element from the heap. Panics if there are no elements.
func (h *Heap[T]) Pop() T {
	n := h.Len() - 1
	if n < 0 {
		panic("no elements in the heap")
	}

	h.swap(0, n)
	h.down(0, n)

	item := h.items[n]
	h.items = h.items[:n]

	return item
}

// Peek returns the top value without removing it from the heap. Will panic if the heap is empty.
func (h *Heap[T]) Peek() T {
	return h.items[0]
}

// Reset removes all elements from the heap (without reducing the capacity).
func (h *Heap[T]) Reset() {
	h.items = h.items[:0]
}
