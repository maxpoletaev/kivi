package service

import (
	"github.com/maxpoletaev/kv/membership/proto"
)

type MembershipService struct {
	proto.UnimplementedMembershipServiceServer
	memberlist MemberRegistry
}

func NewMembershipService(memberlist MemberRegistry) *MembershipService {
	return &MembershipService{
		memberlist: memberlist,
	}
}
