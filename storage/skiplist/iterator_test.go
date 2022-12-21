package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterator(t *testing.T) {
	node := &listNode[int, bool]{key: 1}
	node.next[0] = &listNode[int, bool]{key: 2}
	node.next[0].next[0] = &listNode[int, bool]{key: 3}
	node.next[0].next[0].next[0] = &listNode[int, bool]{key: 4}
	node.next[0].next[0].next[0].next[0] = &listNode[int, bool]{key: 5}
	node.next[0].next[0].next[0].next[0].next[0] = &listNode[int, bool]{key: 5, marked: 1}

	intPtr := func(n int) *int { return &n }

	type test struct {
		node         *listNode[int, bool]
		level        int
		stopAt       *int
		wantSequence []int
	}

	tests := map[string]test{
		"AllValues": {
			level:        0,
			node:         node,
			wantSequence: []int{1, 2, 3, 4, 5},
		},
		"StopAtKey3": {
			level:        0,
			node:         node,
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
