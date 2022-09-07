package generics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSum(t *testing.T) {
	assert.Equal(t, 15, Sum(1, 2, 3, 4, 5))
	assert.Equal(t, 0, Sum[int]())
}

func TestMax(t *testing.T) {
	assert.Equal(t, 5, Max(1, 4, 5, 3, 1))

	assert.PanicsWithValue(t, "must have at least one value", func() {
		Max[int]()
	})
}

func TestMin(t *testing.T) {
	assert.Equal(t, -5, Min(1, 4, -1, 10, -5, 12, 140))

	assert.PanicsWithValue(t, "must have at least one value", func() {
		Min[int]()
	})
}
