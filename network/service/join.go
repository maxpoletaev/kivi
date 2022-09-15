package service

import (
	"context"
	"fmt"

	"github.com/maxpoletaev/kv/network"
	"github.com/maxpoletaev/kv/network/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *ClusterService) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	localNodes := make([]*proto.Node, 0)
	for _, node := range s.cluster.Nodes() {
		localNodes = append(localNodes, network.ToProtoNode(&node.Node))
	}

	remoteNodes := make([]network.Node, 0, len(req.LocalNodes))
	for _, node := range req.LocalNodes {
		remoteNodes = append(remoteNodes, network.FromProtoNode(node))
	}

	if err := s.cluster.AddMembers(remoteNodes); err != nil {
		status.New(codes.Internal, fmt.Sprintf("failed to add member to the cluster: %v", err))
	}

	s.cluster.Connect()

	return &proto.JoinResponse{
		RemoteNodes: localNodes,
	}, nil
}
