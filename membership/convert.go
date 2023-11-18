package membership

import (
	"github.com/maxpoletaev/kivi/membership/proto"
)

var FromProtoStatusMap = map[proto.Status]Status{
	proto.Status_HEALTHY:   StatusHealthy,
	proto.Status_UNHEALTHY: StatusUnhealthy,
	proto.Status_LEFT:      StatusLeft,
}

func FromProtoNode(node *proto.Node) Node {
	return Node{
		ID:         NodeID(node.Id),
		Status:     FromProtoStatusMap[node.Status],
		Gen:        node.Generation,
		PublicAddr: node.Address,
		Error:      node.Error,
		RunID:      node.RunId,
	}
}

func FromProtoNodeList(nodes []*proto.Node) []Node {
	res := make([]Node, len(nodes))
	for i, node := range nodes {
		res[i] = FromProtoNode(node)
	}

	return res
}

var ToProtoStatusMap = map[Status]proto.Status{
	StatusHealthy:   proto.Status_HEALTHY,
	StatusUnhealthy: proto.Status_UNHEALTHY,
	StatusLeft:      proto.Status_LEFT,
}

func ToProtoNode(node *Node) *proto.Node {
	return &proto.Node{
		Id:         uint32(node.ID),
		Name:       node.Name,
		Address:    node.PublicAddr,
		RunId:      node.RunID,
		Generation: node.Gen,
		Error:      node.Error,
		Status:     ToProtoStatusMap[node.Status],
	}
}

func ToProtoNodeList(nodes []Node) []*proto.Node {
	res := make([]*proto.Node, len(nodes))
	for i, node := range nodes {
		res[i] = ToProtoNode(&node)
	}

	return res
}
