package membership

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCluster_ApplyState(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = 1

	cluster := NewCluster(config)
	cluster.ApplyState([]Node{
		{ID: 1, Status: StatusHealthy, Gen: 1},
		{ID: 2, Status: StatusUnhealthy, Gen: 1},
	}, 0)

	nodes := cluster.Nodes()
	require.Len(t, nodes, 2)

	require.Equal(t, StatusHealthy, nodes[0].Status)
	require.Equal(t, NodeID(1), nodes[0].ID)
	require.Equal(t, uint32(1), nodes[0].Gen)
}
