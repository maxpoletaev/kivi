package membership

import (
	"github.com/maxpoletaev/kv/membership/proto"
)

// ToProtoStatus converts a Status to a proto.Status.
func ToProtoStatus(s Status) proto.Status {
	switch s {
	case StatusHealthy:
		return proto.Status_Healthy
	case StatusFaulty:
		return proto.Status_Faulty
	default:
		panic("ToStatusProto: unknown node status")
	}
}

// ToProtoMember converts a Member to a proto.Member.
func ToProtoMember(n *Member) *proto.Member {
	return &proto.Member{
		Id:         uint32(n.ID),
		Name:       n.Name,
		GossipAddr: n.GossipAddr,
		ServerAddr: n.ServerAddr,
		Version:    n.Version,
		Status:     ToProtoStatus(n.Status),
	}
}

// ToProtoMembers converts a list of Member to a list of proto.Member.
func ToProtoMembers(members []Member) []*proto.Member {
	result := make([]*proto.Member, len(members))
	for i, member := range members {
		result[i] = ToProtoMember(&member)
	}

	return result
}

// FromProtoStatus converts a proto.Status to a Status.
func FromProtoStatus(s proto.Status) Status {
	switch s {
	case proto.Status_Healthy:
		return StatusHealthy
	case proto.Status_Faulty:
		return StatusFaulty
	default:
		panic("FromStatusProto: unknown node status")
	}
}

// FromProtoMember converts a proto.Member to a Member.
func FromProtoMember(n *proto.Member) Member {
	return Member{
		ID:         NodeID(n.Id),
		Name:       n.Name,
		GossipAddr: n.GossipAddr,
		ServerAddr: n.ServerAddr,
		Version:    n.Version,
		Status:     FromProtoStatus(n.Status),
	}
}

// FromProtoMembers converts a list of proto.Member to a list of Member.
func FromProtoMembers(members []*proto.Member) []Member {
	result := make([]Member, len(members))
	for i, member := range members {
		result[i] = FromProtoMember(member)
	}

	return result
}

// FromProtoEvent converts a proto.ClusterEvent to a ClusterEvent.
func FromProtoEvent(pe *proto.ClusterEvent) ClusterEvent {
	switch event := pe.Event.(type) {
	case *proto.ClusterEvent_MemberJoined:
		return &MemberJoined{
			ID:         NodeID(event.MemberJoined.MemberId),
			Name:       event.MemberJoined.MemberName,
			ServerAddr: event.MemberJoined.ServerAddr,
			GossipAddr: event.MemberJoined.GossipAddr,
		}
	case *proto.ClusterEvent_MemberLeft:
		return &MemberLeft{
			ID:       NodeID(event.MemberLeft.MemberId),
			SourceID: NodeID(event.MemberLeft.SourceMemberId),
		}
	case *proto.ClusterEvent_MemberUpdated:
		return &MemberUpdated{
			Version:  event.MemberUpdated.Version,
			ID:       NodeID(event.MemberUpdated.MemberId),
			Status:   FromProtoStatus(event.MemberUpdated.Status),
			SourceID: NodeID(event.MemberUpdated.SourceMemberId),
		}
	default:
		panic("FromEventProto: unknown event type")
	}
}

// ToProtoEvent converts a ClusterEvent to a proto.ClusterEvent.
func ToProtoEvent(e ClusterEvent) *proto.ClusterEvent {
	switch event := e.(type) {
	case *MemberJoined:
		return &proto.ClusterEvent{
			Event: &proto.ClusterEvent_MemberJoined{
				MemberJoined: &proto.MemberJoinedEvent{
					MemberId:   uint32(event.ID),
					MemberName: event.Name,
					GossipAddr: event.GossipAddr,
					ServerAddr: event.ServerAddr,
				},
			},
		}
	case *MemberLeft:
		return &proto.ClusterEvent{
			Event: &proto.ClusterEvent_MemberLeft{
				MemberLeft: &proto.MemberLeftEvent{
					MemberId:       uint32(event.ID),
					SourceMemberId: uint32(event.SourceID),
				},
			},
		}
	case *MemberUpdated:
		return &proto.ClusterEvent{
			Event: &proto.ClusterEvent_MemberUpdated{
				MemberUpdated: &proto.MemberUpdatedEvent{
					Version:        event.Version,
					MemberId:       uint32(event.ID),
					SourceMemberId: uint32(event.SourceID),
					Status:         ToProtoStatus(event.Status),
				},
			},
		}
	default:
		panic("ToEventProto: unknown event type")
	}
}
