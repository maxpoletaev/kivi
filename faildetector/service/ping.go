package service

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kv/faildetector/proto"
	"github.com/maxpoletaev/kv/membership"
)

func (s *FailDetectorService) Ping(ctx context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	memberID := membership.NodeID(req.MemberId)
	self := s.members.Self()

	if memberID == 0 || memberID == self.ID {
		// Always report self as healthy.
		return &proto.PingResponse{
			Alive:  true,
			TookMs: 0,
		}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	start := time.Now()

	conn, err := s.connections.Get(membership.NodeID(req.MemberId))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to connect to member %d: %v", req.MemberId, err)
	}

	var (
		alive   bool
		timeout bool
	)

	if resp, err := conn.PingDirect(ctx); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			timeout = true
		}
	} else {
		alive = resp.Alive
	}

	return &proto.PingResponse{
		Alive:   alive,
		Timeout: timeout,
		TookMs:  time.Since(start).Milliseconds(),
	}, nil
}
