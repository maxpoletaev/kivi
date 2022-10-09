package service

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/maxpoletaev/kv/clustering"
	"github.com/maxpoletaev/kv/clustering/proto"
)

func (s *ClusterService) Info(ctx context.Context, _ *emptypb.Empty) (*proto.ClusterInfo, error) {
	members := make([]*proto.Member, 0)
	for _, n := range s.cluster.Nodes() {
		members = append(members, clustering.ToProtoMember(&n.Member))
	}

	return &proto.ClusterInfo{
		Members: members,
	}, nil
}
