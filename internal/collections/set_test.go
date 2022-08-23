package collections

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet_And(t *testing.T) {
	s1 := New(1, 2, 3)
	s2 := New(3, 4, 5)

	assert.Equal(t, New(1, 2, 3), s1)
	assert.Equal(t, New(3, 4, 5), s2)
	assert.Equal(t, New(1, 2, 3, 4, 5), s1.And(s2))
}

func TestSet_Or(t *testing.T) {
	s1 := New(1, 2, 3, 4)
	s2 := New(3, 4, 5, 6)

	assert.Equal(t, New(1, 2, 3, 4), s1)
	assert.Equal(t, New(3, 4, 5, 6), s2)
	assert.Equal(t, New(3, 4), s1.Or(s2))
}
