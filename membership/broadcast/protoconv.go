package broadcast

import (
	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
)

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

func fromProtoEvent(pe *proto.ClusterEvent) membership.ClusterEvent {
	switch event := pe.Event.(type) {
	case *proto.ClusterEvent_MemberJoined:
		return &membership.MemberJoined{
			ID:         membership.NodeID(event.MemberJoined.MemberId),
			Name:       event.MemberJoined.MemberName,
			ServerAddr: event.MemberJoined.ServerAddr,
			GossipAddr: event.MemberJoined.GossipAddr,
		}
	case *proto.ClusterEvent_MemberLeft:
		return &membership.MemberLeft{
			ID:       membership.NodeID(event.MemberLeft.MemberId),
			SourceID: membership.NodeID(event.MemberLeft.SourceMemberId),
		}
	case *proto.ClusterEvent_MemberUpdated:
		return &membership.MemberUpdated{
			Version:  event.MemberUpdated.Version,
			ID:       membership.NodeID(event.MemberUpdated.MemberId),
			Status:   fromProtoStatus(event.MemberUpdated.Status),
			SourceID: membership.NodeID(event.MemberUpdated.SourceMemberId),
		}
	default:
		panic("FromEventProto: unknown event type")
	}
}

func toProtoEvent(e membership.ClusterEvent) *proto.ClusterEvent {
	switch event := e.(type) {
	case *membership.MemberJoined:
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
	case *membership.MemberLeft:
		return &proto.ClusterEvent{
			Event: &proto.ClusterEvent_MemberLeft{
				MemberLeft: &proto.MemberLeftEvent{
					MemberId:       uint32(event.ID),
					SourceMemberId: uint32(event.SourceID),
				},
			},
		}
	case *membership.MemberUpdated:
		return &proto.ClusterEvent{
			Event: &proto.ClusterEvent_MemberUpdated{
				MemberUpdated: &proto.MemberUpdatedEvent{
					Version:        event.Version,
					MemberId:       uint32(event.ID),
					SourceMemberId: uint32(event.SourceID),
					Status:         toProtoStatus(event.Status),
				},
			},
		}
	default:
		panic("ToEventProto: unknown event type")
	}
}
