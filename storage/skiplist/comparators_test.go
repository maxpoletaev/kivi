package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderedComparator(t *testing.T) {
	assert.Equal(t, 1, OrderedComparator(2, 1))
	assert.Equal(t, -1, OrderedComparator(1, 2))
	assert.Equal(t, 0, OrderedComparator(2, 2))

	assert.Equal(t, 1, OrderedComparator("bbb", "aaa"))
	assert.Equal(t, -1, OrderedComparator("ccc", "ddd"))
	assert.Equal(t, 0, OrderedComparator("abc", "abc"))
}
