package bloom

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddAndCheck(t *testing.T) {
	var value [10]byte
	bf := New(value[:], 3)

	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))

	assert.True(t, bf.MayContain([]byte("hello")))
	assert.True(t, bf.MayContain([]byte("world")))
	assert.False(t, bf.MayContain([]byte("foo")))
	assert.False(t, bf.MayContain([]byte("bar")))
}
