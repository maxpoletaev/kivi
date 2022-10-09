package service

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/maxpoletaev/kv/clustering"
	"github.com/maxpoletaev/kv/clustering/proto"
)

func (s *ClusterService) PingIndirect(ctx context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	node := s.cluster.NodeByID(clustering.NodeID(req.NodeId))
	if node == nil {
		return nil, status.New(
			codes.NotFound, "no such node",
		).Err()
	}

	start := time.Now()

	_, err := node.Ping(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	return &proto.PingResponse{
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}
