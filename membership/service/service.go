package service

import (
	"context"

	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/membership/proto"
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
	remoteNodes := fromProtoNodes(req.Nodes)
	sourceID := membership.NodeID(req.NodeId)
	localNodes := s.cluster.ApplyState(remoteNodes, sourceID)

	return &proto.PullPushStateResponse{
		Nodes: toProtoNodes(localNodes),
	}, nil
}

func (s *MembershipServer) GetStateHash(ctx context.Context, req *proto.GetStateHashRequest) (*proto.GetStateHashResponse, error) {
	return &proto.GetStateHashResponse{
		Hash: s.cluster.StateHash(),
	}, nil
}
