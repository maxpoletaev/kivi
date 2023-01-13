package service

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/maxpoletaev/kiwi/membership/proto"
)

func (s *MembershipService) Members(ctx context.Context, _ *emptypb.Empty) (*proto.MembersResponse, error) {
	members := toProtoMembers(s.memberlist.Members())

	return &proto.MembersResponse{
		Members: members,
	}, nil
}
