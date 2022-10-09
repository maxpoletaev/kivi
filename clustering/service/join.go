package service

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kv/clustering"
	"github.com/maxpoletaev/kv/clustering/proto"
)

func (s *ClusterService) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	localMembers := make([]*proto.Member, 0)
	for _, node := range s.cluster.Nodes() {
		localMembers = append(localMembers, clustering.ToProtoMember(&node.Member))
	}

	remoteMembers := make([]clustering.Member, 0, len(req.LocalMembers))
	for _, node := range req.LocalMembers {
		remoteMembers = append(remoteMembers, clustering.FromProtoMember(node))
	}

	err := s.cluster.AddMembers(remoteMembers)
	if err != nil {
		return nil, status.New(
			codes.Internal, fmt.Sprintf("failed to add members to the cluster: %v", err),
		).Err()
	}

	return &proto.JoinResponse{
		RemoteMembers: localMembers,
	}, nil
}
