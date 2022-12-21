package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListNode_LoadNext(t *testing.T) {
	node := &listNode[int, string]{
		next: listNodes[int, string]{
			{
				key:    1,
				marked: 1,
				next: listNodes[int, string]{
					{
						key:    2,
						marked: 0,
						next: listNodes[int, string]{
							{
								key:    3,
								marked: 1,
								next: listNodes[int, string]{
									{
										key:    4,
										marked: 0,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	actualKeys := []int{}
	for node := node.loadNext(0); node != nil; node = node.loadNext(0) {
		actualKeys = append(actualKeys, node.key)
	}

	assert.Equal(t, []int{2, 4}, actualKeys)
}
