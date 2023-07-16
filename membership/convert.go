package membership

import (
	"github.com/maxpoletaev/kivi/nodeapi"
)

func fromAPINodeInfo(nodeinfo *nodeapi.NodeInfo) Node {
	var status Status

	switch nodeinfo.Status {
	case nodeapi.NodeStatusHealthy:
		status = StatusHealthy
	case nodeapi.NodeStatusUnhealthy:
		status = StatusUnhealthy
	case nodeapi.NodeStatusLeft:
		status = StatusLeft
	}

	return Node{
		ID:         NodeID(nodeinfo.ID),
		Name:       nodeinfo.Name,
		Gen:        nodeinfo.Gen,
		PublicAddr: nodeinfo.Addr,
		Status:     status,
		Error:      nodeinfo.Error,
		RunID:      nodeinfo.RunID,
	}
}

func fromAPINodesInfo(nodesInfo []nodeapi.NodeInfo) []Node {
	nodes := make([]Node, len(nodesInfo))
	for i, nodeInfo := range nodesInfo {
		nodes[i] = fromAPINodeInfo(&nodeInfo)
	}

	return nodes
}

func toAPINodeInfo(node *Node) nodeapi.NodeInfo {
	var status nodeapi.NodeStatus

	switch node.Status {
	case StatusHealthy:
		status = nodeapi.NodeStatusHealthy
	case StatusUnhealthy:
		status = nodeapi.NodeStatusUnhealthy
	case StatusLeft:
		status = nodeapi.NodeStatusLeft
	}

	return nodeapi.NodeInfo{
		ID:     nodeapi.NodeID(node.ID),
		Name:   node.Name,
		Gen:    node.Gen,
		Addr:   node.PublicAddr,
		Status: status,
		Error:  node.Error,
		RunID:  node.RunID,
	}
}

func toAPINodesInfo(nodes []Node) []nodeapi.NodeInfo {
	nodesInfo := make([]nodeapi.NodeInfo, len(nodes))
	for i, node := range nodes {
		nodesInfo[i] = toAPINodeInfo(&node)
	}

	return nodesInfo
}
