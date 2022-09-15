package service

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/maxpoletaev/kv/network"
	"github.com/maxpoletaev/kv/network/proto"
)

func (s *ClusterService) Info(ctx context.Context, _ *emptypb.Empty) (*proto.NodeList, error) {
	nodes := make([]*proto.Node, 0)
	for _, n := range s.cluster.Nodes() {
		nodes = append(nodes, network.ToProtoNode(&n.Node))
	}

	return &proto.NodeList{
		Nodes: nodes,
	}, nil
}
