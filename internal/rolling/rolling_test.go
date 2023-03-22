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
	var maxInt8 = math.MaxInt8
	assert.Equal(t, Less, Compare(maxInt8, maxInt8+1))
	assert.Equal(t, Greater, Compare(maxInt8+1, maxInt8))
	assert.Equal(t, Equal, Compare(maxInt8+1, maxInt8+1))
}
