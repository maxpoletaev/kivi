package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func intPtr(i int) *int {
	return &i
}

func TestIterator(t *testing.T) {
	head := &listNode[int, bool]{key: 1}

	// Generate a list with 5 nodes: 1->2->3->4->5.
	for node, i := head, 2; i <= 5; i++ {
		nx := &listNode[int, bool]{key: i}
		node.storeNext(0, nx)
		node = nx
	}

	type test struct {
		node         *listNode[int, bool]
		level        int
		stopAt       *int
		wantSequence []int
	}

	tests := map[string]test{
		"AllValues": {
			level:        0,
			node:         head,
			wantSequence: []int{1, 2, 3, 4, 5},
		},
		"StopAtKey3": {
			level:        0,
			node:         head,
			stopAt:       intPtr(3),
			wantSequence: []int{1, 2, 3},
		},
		"EmptyList": {
			level:        0,
			node:         nil,
			wantSequence: []int{},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			sequence := make([]int, 0)

			it := newIterator(tt.node, tt.level, IntComparator, tt.stopAt)

			for it.HasNext() {
				key, _ := it.Next()
				sequence = append(sequence, key)
			}

			assert.Equal(t, tt.wantSequence, sequence)
		})
	}
}
