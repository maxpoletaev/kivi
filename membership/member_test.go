package membership

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeID_String(t *testing.T) {
	assert.Equal(t, "1", NodeID(1).String())
}

func TestMember_IsReachable(t *testing.T) {
	m := &Member{Status: StatusHealthy}
	assert.True(t, m.IsReachable())

	m.Status = StatusFaulty
	assert.False(t, m.IsReachable())

	m.Status = Status(0)
	assert.False(t, m.IsReachable())
}
