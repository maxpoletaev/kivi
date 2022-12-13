package service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
)

func (s *MembershipService) Expell(ctx context.Context, req *proto.ExpellRequest) (*emptypb.Empty, error) {
	if err := s.memberlist.Expell(membership.NodeID(req.MemberId)); err != nil {
		st := status.New(codes.Internal, err.Error())
		if errors.Is(err, membership.ErrMemberNotFound) {
			st = status.New(codes.NotFound, err.Error())
		}

		return nil, st.Err()
	}

	return &emptypb.Empty{}, nil
}
