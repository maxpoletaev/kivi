package membership

import "github.com/maxpoletaev/kivi/nodeclient"

func fromApiNodeInfo(nodeinfo *nodeclient.NodeInfo) Node {
	var status Status

	switch nodeinfo.Status {
	case nodeclient.NodeStatusHealthy:
		status = StatusHealthy
	case nodeclient.NodeStatusUnhealthy:
		status = StatusUnhealthy
	case nodeclient.NodeStatusLeft:
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

func fromApiNodesInfo(nodesInfo []nodeclient.NodeInfo) []Node {
	nodes := make([]Node, len(nodesInfo))
	for i, nodeInfo := range nodesInfo {
		nodes[i] = fromApiNodeInfo(&nodeInfo)
	}

	return nodes
}

func toApiNodeInfo(node *Node) nodeclient.NodeInfo {
	var status nodeclient.NodeStatus

	switch node.Status {
	case StatusHealthy:
		status = nodeclient.NodeStatusHealthy
	case StatusUnhealthy:
		status = nodeclient.NodeStatusUnhealthy
	case StatusLeft:
		status = nodeclient.NodeStatusLeft
	}

	return nodeclient.NodeInfo{
		ID:     nodeclient.NodeID(node.ID),
		Name:   node.Name,
		Gen:    node.Gen,
		Addr:   node.PublicAddr,
		Status: status,
		Error:  node.Error,
		RunID:  node.RunID,
	}
}

func toApiNodesInfo(nodes []Node) []nodeclient.NodeInfo {
	nodesInfo := make([]nodeclient.NodeInfo, len(nodes))
	for i, node := range nodes {
		nodesInfo[i] = toApiNodeInfo(&node)
	}

	return nodesInfo
}
