package skiplist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderedComparator(t *testing.T) {
	assert.Equal(t, 1, orderedComparator(2, 1))
	assert.Equal(t, -1, orderedComparator(1, 2))
	assert.Equal(t, 0, orderedComparator(2, 2))

	assert.Equal(t, 1, orderedComparator("bbb", "aaa"))
	assert.Equal(t, -1, orderedComparator("ccc", "ddd"))
	assert.Equal(t, 0, orderedComparator("abc", "abc"))
}
