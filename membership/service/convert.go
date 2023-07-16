package service

import (
	"fmt"

	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/membership/proto"
)

func fromProtoNode(node *proto.Node) membership.Node {
	var status membership.Status

	switch node.Status {
	case proto.Status_HEALTHY:
		status = membership.StatusHealthy
	case proto.Status_UNHEALTHY:
		status = membership.StatusUnhealthy
	case proto.Status_LEFT:
		status = membership.StatusLeft
	}

	return membership.Node{
		ID:         membership.NodeID(node.Id),
		Gen:        node.Generation,
		PublicAddr: node.Address,
		Status:     status,
		Error:      node.Error,
		RunID:      node.RunId,
	}
}

func fromProtoNodes(nodes []*proto.Node) []membership.Node {
	res := make([]membership.Node, len(nodes))
	for i, node := range nodes {
		res[i] = fromProtoNode(node)
	}

	return res
}

func toProtoStatus(status membership.Status) proto.Status {
	switch status {
	case membership.StatusHealthy:
		return proto.Status_HEALTHY
	case membership.StatusUnhealthy:
		return proto.Status_UNHEALTHY
	case membership.StatusLeft:
		return proto.Status_LEFT
	default:
		panic(fmt.Sprintf("unknown status %v", status))
	}
}

func toProtoNode(node *membership.Node) *proto.Node {
	return &proto.Node{
		Id:         uint32(node.ID),
		Name:       node.Name,
		Address:    node.PublicAddr,
		RunId:      node.RunID,
		Generation: node.Gen,
		Status:     toProtoStatus(node.Status),
		Error:      node.Error,
	}
}

func toProtoNodes(nodes []membership.Node) []*proto.Node {
	res := make([]*proto.Node, len(nodes))
	for i, node := range nodes {
		res[i] = toProtoNode(&node)
	}

	return res
}
