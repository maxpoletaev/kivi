package skiplist

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestListNode_LoadNext(t *testing.T) {
	node := &listNode[int, bool]{key: 1}
	nextNode := &listNode[int, bool]{key: 2}
	node.next[0] = unsafe.Pointer(nextNode)

	assert.Equal(t, nextNode, node.loadNext(0))
	assert.Nil(t, nextNode.loadNext(1))
}
