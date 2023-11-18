package service

import (
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/membership/proto"
)

var fromProtoStatus = map[proto.Status]membership.Status{
	proto.Status_HEALTHY:   membership.StatusHealthy,
	proto.Status_UNHEALTHY: membership.StatusUnhealthy,
	proto.Status_LEFT:      membership.StatusLeft,
}

func fromProtoNode(node *proto.Node) membership.Node {
	return membership.Node{
		ID:         membership.NodeID(node.Id),
		Status:     fromProtoStatus[node.Status],
		Gen:        node.Generation,
		PublicAddr: node.Address,
		Error:      node.Error,
		RunID:      node.RunId,
	}
}

func fromProtoNodeList(nodes []*proto.Node) []membership.Node {
	res := make([]membership.Node, len(nodes))
	for i, node := range nodes {
		res[i] = fromProtoNode(node)
	}

	return res
}

var toProtoStatus = map[membership.Status]proto.Status{
	membership.StatusHealthy:   proto.Status_HEALTHY,
	membership.StatusUnhealthy: proto.Status_UNHEALTHY,
	membership.StatusLeft:      proto.Status_LEFT,
}

func toProtoNode(node *membership.Node) *proto.Node {
	return &proto.Node{
		Id:         uint32(node.ID),
		Name:       node.Name,
		Address:    node.PublicAddr,
		RunId:      node.RunID,
		Generation: node.Gen,
		Error:      node.Error,
		Status:     toProtoStatus[node.Status],
	}
}

func toProtoNodeList(nodes []membership.Node) []*proto.Node {
	res := make([]*proto.Node, len(nodes))
	for i, node := range nodes {
		res[i] = toProtoNode(&node)
	}

	return res
}
