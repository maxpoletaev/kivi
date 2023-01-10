package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kv/membership/proto"
)

func validateMember(member *proto.Member) error {
	if member == nil {
		return status.Newf(codes.InvalidArgument, "member is nil").Err()
	}

	if member.Id == 0 {
		return status.Newf(codes.InvalidArgument, "member ID is zero").Err()
	}

	if member.Name == "" {
		return status.Newf(codes.InvalidArgument, "member name is empty").Err()
	}

	if member.ServerAddr == "" {
		return status.Newf(codes.InvalidArgument, "member server address is empty").Err()
	}

	if member.GossipAddr == "" {
		return status.Newf(codes.InvalidArgument, "member gossip address is empty").Err()
	}

	if member.Version == 0 {
		return status.Newf(codes.InvalidArgument, "member version is zero").Err()
	}

	return nil
}

func validateJoinRequest(req *proto.JoinRequest) error {
	if req == nil {
		return status.Newf(codes.InvalidArgument, "request is nil").Err()
	}

	for i, member := range req.MembersToAdd {
		if err := validateMember(member); err != nil {
			return status.Newf(
				codes.InvalidArgument, "invalid member at index %d: %s", i, err,
			).Err()
		}
	}

	return nil
}

func (s *MembershipService) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	if err := validateJoinRequest(req); err != nil {
		return nil, err
	}

	membersToAdd := fromProtoMembers(req.MembersToAdd)
	if err := s.memberlist.Add(membersToAdd...); err != nil {
		return nil, status.Newf(
			codes.Internal, "failed to add members to the cluster: %s", err,
		).Err()
	}

	localMembers := toProtoMembers(s.memberlist.Members())
	return &proto.JoinResponse{
		Members: localMembers,
	}, nil
}
