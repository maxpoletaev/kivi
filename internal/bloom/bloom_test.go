package bloom

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddAndCheck(t *testing.T) {
	var value uint64
	bf := New(value, 3)

	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))

	assert.True(t, bf.Check([]byte("hello")))
	assert.True(t, bf.Check([]byte("world")))
	assert.False(t, bf.Check([]byte("foo")))
	assert.False(t, bf.Check([]byte("bar")))
}
