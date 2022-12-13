package membership

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatus_String(t *testing.T) {
	assert.Equal(t, "", Status(-1).String())
	assert.Equal(t, "faulty", StatusFaulty.String())
	assert.Equal(t, "healthy", StatusHealthy.String())
}
