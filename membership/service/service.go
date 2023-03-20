package service

import (
	"context"

	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/membership/proto"
)

type MembershipServer struct {
	proto.UnsafeMembershipServer
	cluster Cluster
}

func NewMembershipServer(cluster Cluster) *MembershipServer {
	return &MembershipServer{
		cluster: cluster,
	}
}

func (s *MembershipServer) ListNodes(ctx context.Context, req *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	nodes := s.cluster.Nodes()

	return &proto.ListNodesResponse{
		Nodes: toProtoNodes(nodes),
	}, nil
}

func (s *MembershipServer) PullPushState(ctx context.Context, req *proto.PullPushStateRequest) (*proto.PullPushStateResponse, error) {
	nodes := s.cluster.ApplyState(fromProtoNodes(req.Nodes))

	return &proto.PullPushStateResponse{
		Nodes: toProtoNodes(nodes),
	}, nil
}

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
		Generation: node.Generation,
		Address:    node.Address,
		Status:     status,
	}
}

func fromProtoNodes(nodes []*proto.Node) []membership.Node {
	res := make([]membership.Node, len(nodes))
	for i, node := range nodes {
		res[i] = fromProtoNode(node)
	}

	return res
}

func toProtoNode(node *membership.Node) *proto.Node {
	var status proto.Status

	switch node.Status {
	case membership.StatusHealthy:
		status = proto.Status_HEALTHY
	case membership.StatusUnhealthy:
		status = proto.Status_UNHEALTHY
	case membership.StatusLeft:
		status = proto.Status_LEFT
	}

	return &proto.Node{
		Id:         uint32(node.ID),
		Generation: node.Generation,
		Address:    node.Address,
		Status:     status,
	}
}

func toProtoNodes(nodes []membership.Node) []*proto.Node {
	res := make([]*proto.Node, len(nodes))
	for i, node := range nodes {
		res[i] = toProtoNode(&node)
	}

	return res
}
