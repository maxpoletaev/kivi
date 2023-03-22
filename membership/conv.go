package membership

import "github.com/maxpoletaev/kivi/nodeapi"

func fromApiNodeInfo(nodeinfo *nodeapi.NodeInfo) Node {
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
		Gen:        nodeinfo.Gen,
		PublicAddr: nodeinfo.Addr,
		Status:     status,
		Error:      nodeinfo.Error,
		RunID:      nodeinfo.RunID,
	}
}

func fromApiNodesInfo(nodesInfo []nodeapi.NodeInfo) []Node {
	nodes := make([]Node, len(nodesInfo))
	for i, nodeInfo := range nodesInfo {
		nodes[i] = fromApiNodeInfo(&nodeInfo)
	}

	return nodes
}

func toApiNodeInfo(node *Node) nodeapi.NodeInfo {
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
		Gen:    node.Gen,
		Addr:   node.PublicAddr,
		Status: status,
		Error:  node.Error,
		RunID:  node.RunID,
	}
}

func toApiNodesInfo(nodes []Node) []nodeapi.NodeInfo {
	nodesInfo := make([]nodeapi.NodeInfo, len(nodes))
	for i, node := range nodes {
		nodesInfo[i] = toApiNodeInfo(&node)
	}

	return nodesInfo
}
