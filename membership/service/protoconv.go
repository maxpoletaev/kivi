package service

import (
	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
)

func toProtoStatus(s membership.Status) proto.Status {
	switch s {
	case membership.StatusHealthy:
		return proto.Status_Healthy
	case membership.StatusFaulty:
		return proto.Status_Faulty
	default:
		panic("ToStatusProto: unknown node status")
	}
}

func toProtoMember(n *membership.Member) *proto.Member {
	return &proto.Member{
		Id:         uint32(n.ID),
		Name:       n.Name,
		GossipAddr: n.GossipAddr,
		ServerAddr: n.ServerAddr,
		Version:    n.Version,
		Status:     toProtoStatus(n.Status),
	}
}

func toProtoMembers(members []membership.Member) []*proto.Member {
	result := make([]*proto.Member, len(members))
	for i, member := range members {
		result[i] = toProtoMember(&member)
	}

	return result
}

func fromProtoStatus(s proto.Status) membership.Status {
	switch s {
	case proto.Status_Healthy:
		return membership.StatusHealthy
	case proto.Status_Faulty:
		return membership.StatusFaulty
	default:
		panic("FromStatusProto: unknown node status")
	}
}

func fromProtoMember(n *proto.Member) membership.Member {
	return membership.Member{
		ID:         membership.NodeID(n.Id),
		Name:       n.Name,
		GossipAddr: n.GossipAddr,
		ServerAddr: n.ServerAddr,
		Version:    n.Version,
		Status:     fromProtoStatus(n.Status),
	}
}

func fromProtoMembers(members []*proto.Member) []membership.Member {
	result := make([]membership.Member, len(members))
	for i, member := range members {
		result[i] = fromProtoMember(member)
	}

	return result
}
