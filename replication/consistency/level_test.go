package consistency

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsistencyLevel_Count(t *testing.T) {
	assert.Equal(t, 1, One.N(9))
	assert.Equal(t, 2, Two.N(9))
	assert.Equal(t, 5, Quorum.N(9))
	assert.Equal(t, 9, All.N(9))
}
