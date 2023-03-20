package membership_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kiwi/membership"
)

func TestCluster_ApplyState(t *testing.T) {
	cluster := membership.NewCluster(membership.Node{ID: 1}, membership.DefaultConfig())

	cluster.ApplyState([]membership.Node{
		{ID: 1, Status: membership.StatusHealthy, Generation: 1},
		{ID: 2, Status: membership.StatusUnhealthy, Generation: 1},
	})

	nodes := cluster.Nodes()
	require.Len(t, nodes, 2)

	require.Equal(t, membership.StatusHealthy, nodes[0].Status)
	require.Equal(t, membership.NodeID(1), nodes[0].ID)
	require.Equal(t, int32(1), nodes[0].Generation)
}
