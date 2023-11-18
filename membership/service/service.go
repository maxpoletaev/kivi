package service

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/membership/proto"
)

type MembershipServer struct {
	proto.UnimplementedMembershipServer
	cluster membership.Cluster
}

func NewMembershipService(cluster membership.Cluster) *MembershipServer {
	return &MembershipServer{
		cluster: cluster,
	}
}

func (s *MembershipServer) ListNodes(ctx context.Context, req *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	nodes := s.cluster.Nodes()

	return &proto.ListNodesResponse{
		Nodes: toProtoNodeList(nodes),
	}, nil
}

func (s *MembershipServer) PullPushState(ctx context.Context, req *proto.PullPushStateRequest) (*proto.PullPushStateResponse, error) {
	remoteNodes := fromProtoNodeList(req.Nodes)
	sourceID := membership.NodeID(req.NodeId)
	localNodes := s.cluster.ApplyState(remoteNodes, sourceID)

	return &proto.PullPushStateResponse{
		Nodes: toProtoNodeList(localNodes),
	}, nil
}

func (s *MembershipServer) Ping(ctx context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	return &proto.PingResponse{
		StateHash: s.cluster.StateHash(),
	}, nil
}

func (s *MembershipServer) PingIndirect(ctx context.Context, req *proto.PingIndirectRequest) (*proto.PingIndirectResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Millisecond)
	defer cancel()

	start := time.Now()
	targetID := membership.NodeID(req.NodeId)

	if _, ok := s.cluster.Node(targetID); !ok {
		return nil, status.New(
			codes.NotFound, fmt.Sprintf("node %d not found", targetID),
		).Err()
	}

	conn, err := s.cluster.ConnContext(ctx, targetID)
	if err != nil {
		return &proto.PingIndirectResponse{
			Status:  proto.Status_UNHEALTHY,
			Message: fmt.Sprintf("failed to connect: %s", err),
		}, nil
	}

	_, err = conn.Ping(ctx)
	if err != nil {
		return &proto.PingIndirectResponse{
			Status:  proto.Status_UNHEALTHY,
			Message: fmt.Sprintf("failed to ping: %s", err),
		}, nil
	}

	return &proto.PingIndirectResponse{
		Duration: time.Since(start).Milliseconds(),
		Status:   proto.Status_HEALTHY,
	}, nil
}
