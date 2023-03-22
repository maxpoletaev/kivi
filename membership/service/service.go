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
	state := s.cluster.ApplyState(membership.State{
		SourceID: membership.NodeID(req.NodeId),
		Nodes:    fromProtoNodes(req.Nodes),
	})

	return &proto.PullPushStateResponse{
		Nodes: toProtoNodes(state.Nodes),
	}, nil
}
