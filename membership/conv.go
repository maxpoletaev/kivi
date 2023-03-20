package membership

import "github.com/maxpoletaev/kiwi/nodeapi"

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
		Generation: nodeinfo.Gen,
		Address:    nodeinfo.Addr,
		Status:     status,
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
		Gen:    node.Generation,
		Addr:   node.Address,
		Status: status,
	}
}

func toApiNodesInfo(nodes []Node) []nodeapi.NodeInfo {
	nodesInfo := make([]nodeapi.NodeInfo, len(nodes))
	for i, node := range nodes {
		nodesInfo[i] = toApiNodeInfo(&node)
	}

	return nodesInfo
}
