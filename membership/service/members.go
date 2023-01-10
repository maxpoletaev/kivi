package service

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
)

func (s *MembershipService) Members(ctx context.Context, _ *emptypb.Empty) (*proto.MembersResponse, error) {
	members := membership.ToProtoMembers(s.memberlist.Members())

	return &proto.MembersResponse{
		Members: members,
	}, nil
}
