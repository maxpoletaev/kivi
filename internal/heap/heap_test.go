package heap_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/maxpoletaev/kivi/internal/heap"
)

func TestMinHeap(t *testing.T) {
	h := heap.New(heap.Min[int])

	h.Push(3)
	h.Push(1)
	h.Push(2)

	assert.Equal(t, 3, h.Len())

	assert.Equal(t, 1, h.Peek())
	assert.Equal(t, 1, h.Pop())
	assert.Equal(t, 2, h.Len())

	assert.Equal(t, 2, h.Peek())
	assert.Equal(t, 2, h.Pop())
	assert.Equal(t, 1, h.Len())

	assert.Equal(t, 3, h.Peek())
	assert.Equal(t, 3, h.Pop())
	assert.Equal(t, 0, h.Len())
}

func TestMaxHeap(t *testing.T) {
	h := heap.New(heap.Max[int])

	h.Push(3)
	h.Push(1)
	h.Push(2)

	assert.Equal(t, 3, h.Len())

	assert.Equal(t, 3, h.Peek())
	assert.Equal(t, 3, h.Pop())
	assert.Equal(t, 2, h.Len())

	assert.Equal(t, 2, h.Peek())
	assert.Equal(t, 2, h.Pop())
	assert.Equal(t, 1, h.Len())

	assert.Equal(t, 1, h.Peek())
	assert.Equal(t, 1, h.Pop())
	assert.Equal(t, 0, h.Len())
}

func TestPopFromEmpty(t *testing.T) {
	h := heap.New(heap.Max[int])

	assert.PanicsWithValue(t, "no elements in the heap", func() {
		h.Pop()
	})
}
