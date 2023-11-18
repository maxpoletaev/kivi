package membership

import (
	"github.com/maxpoletaev/kivi/noderpc"
)

var fromAPIStatusMap = map[noderpc.NodeStatus]Status{
	noderpc.NodeStatusHealthy:   StatusHealthy,
	noderpc.NodeStatusUnhealthy: StatusUnhealthy,
	noderpc.NodeStatusLeft:      StatusLeft,
}

func fromAPINodeInfo(nodeinfo *noderpc.NodeInfo) Node {
	return Node{
		ID:         NodeID(nodeinfo.ID),
		Name:       nodeinfo.Name,
		Gen:        nodeinfo.Gen,
		PublicAddr: nodeinfo.Addr,
		Error:      nodeinfo.Error,
		RunID:      nodeinfo.RunID,
		Status:     fromAPIStatusMap[nodeinfo.Status],
	}
}

func fromAPINodeInfoList(nodesInfo []noderpc.NodeInfo) []Node {
	nodes := make([]Node, len(nodesInfo))
	for i, nodeInfo := range nodesInfo {
		nodes[i] = fromAPINodeInfo(&nodeInfo)
	}

	return nodes
}

var toAPIStatusMap = map[Status]noderpc.NodeStatus{
	StatusHealthy:   noderpc.NodeStatusHealthy,
	StatusUnhealthy: noderpc.NodeStatusUnhealthy,
	StatusLeft:      noderpc.NodeStatusLeft,
}

func toAPINodeInfo(node *Node) noderpc.NodeInfo {
	return noderpc.NodeInfo{
		ID:     noderpc.NodeID(node.ID),
		Name:   node.Name,
		Gen:    node.Gen,
		Error:  node.Error,
		RunID:  node.RunID,
		Addr:   node.PublicAddr,
		Status: toAPIStatusMap[node.Status],
	}
}

func toAPINodeInfoList(nodes []Node) []noderpc.NodeInfo {
	nodesInfo := make([]noderpc.NodeInfo, len(nodes))
	for i, node := range nodes {
		nodesInfo[i] = toAPINodeInfo(&node)
	}

	return nodesInfo
}
