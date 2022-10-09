package consistency

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsistencyLevel_Count(t *testing.T) {
	assert.Equal(t, 1, LevelOne.N(9))
	assert.Equal(t, 2, LevelTwo.N(9))
	assert.Equal(t, 5, LevelQuorum.N(9))
	assert.Equal(t, 9, LevelAll.N(9))
}
