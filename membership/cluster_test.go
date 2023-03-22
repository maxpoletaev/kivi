package membership_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kiwi/membership"
)

func TestCluster_ApplyState(t *testing.T) {
	cluster := membership.NewCluster(membership.DefaultConfig())

	cluster.ApplyState(membership.State{
		Nodes: []membership.Node{
			{ID: 1, Status: membership.StatusHealthy, Gen: 1},
			{ID: 2, Status: membership.StatusUnhealthy, Gen: 1},
		},
	})

	nodes := cluster.Nodes()
	require.Len(t, nodes, 2)

	require.Equal(t, membership.StatusHealthy, nodes[0].Status)
	require.Equal(t, membership.NodeID(1), nodes[0].ID)
	require.Equal(t, int32(1), nodes[0].Gen)
}
