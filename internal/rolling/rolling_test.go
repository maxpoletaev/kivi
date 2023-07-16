package rolling

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	// Compare within the same sign.
	assert.Equal(t, Less, Compare(0, 1))
	assert.Equal(t, Greater, Compare(1, 0))
	assert.Equal(t, Equal, Compare(1, 1))
	assert.Equal(t, Less, Compare(-2, -1))

	// Compare with rolling.
	maxInt32 := math.MaxInt32
	assert.Equal(t, Less, Compare(maxInt32, maxInt32+1))
	assert.Equal(t, Greater, Compare(maxInt32+1, maxInt32))
	assert.Equal(t, Equal, Compare(maxInt32+1, maxInt32+1))
}
