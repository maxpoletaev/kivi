package service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/membership/proto"
)

func (s *MembershipService) Expel(ctx context.Context, req *proto.ExpelRequest) (*emptypb.Empty, error) {
	if err := s.memberlist.Expel(membership.NodeID(req.MemberId)); err != nil {
		st := status.New(codes.Internal, err.Error())
		if errors.Is(err, membership.ErrNoSuchMember) {
			st = status.New(codes.NotFound, err.Error())
		}

		return nil, st.Err()
	}

	return &emptypb.Empty{}, nil
}
