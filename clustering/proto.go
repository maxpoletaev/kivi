package clustering

import "github.com/maxpoletaev/kv/clustering/proto"

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

func FromProtoStatus(s proto.Status) Status {
	switch s {
	case proto.Status_Alive:
		return StatusAlive
	case proto.Status_Dead:
		return StatusDead
	default:
		panic("unknown node status")
	}
}

func ToProtoStatus(s Status) proto.Status {
	switch s {
	case StatusAlive:
		return proto.Status_Alive
	case StatusDead:
		return proto.Status_Dead
	default:
		panic("unknown node status")
	}
}

func FromProtoEvent(pe *proto.ClusterEvent) ClusterEvent {
	switch event := pe.Event.(type) {
	case *proto.ClusterEvent_Joined:
		node := event.Joined.Member

		return &NodeJoined{
			NodeID:     NodeID(node.Id),
			NodeName:   node.Name,
			ServerAddr: node.ServerAddr,
			GossipAddr: node.GossipAddr,
		}
	case *proto.ClusterEvent_StatusChanged:
		s := event.StatusChanged

		return &NodeStatusChanged{
			Version:   s.Version,
			SourceID:  NodeID(s.SourceId),
			NodeID:    NodeID(s.NodeId),
			Status:    FromProtoStatus(s.Status),
			OldStatus: FromProtoStatus(s.OldStatus),
		}
	case *proto.ClusterEvent_Left:
		return &NodeLeft{
			NodeID: NodeID(event.Left.NodeId),
		}
	default:
		panic("fromProtoEvent: unknown event type")
	}
}

func ToProtoEvent(e ClusterEvent) *proto.ClusterEvent {
	switch event := e.(type) {
	case *NodeJoined:
		return &proto.ClusterEvent{
			Event: &proto.ClusterEvent_Joined{
				Joined: &proto.NodeJoinedEvent{
					Member: &proto.Member{
						Id:         uint32(event.NodeID),
						Name:       event.NodeName,
						GossipAddr: event.GossipAddr,
						ServerAddr: event.ServerAddr,
					},
				},
			},
		}
	case *NodeStatusChanged:
		return &proto.ClusterEvent{
			Event: &proto.ClusterEvent_StatusChanged{
				StatusChanged: &proto.NodeStatusChangedEvent{
					SourceId:  uint32(event.SourceID),
					NodeId:    uint32(event.NodeID),
					Version:   event.Version,
					Status:    ToProtoStatus(event.Status),
					OldStatus: ToProtoStatus(event.OldStatus),
				},
			},
		}
	case *NodeLeft:
		return &proto.ClusterEvent{
			Event: &proto.ClusterEvent_Left{
				Left: &proto.NodeLeftEvent{
					NodeId: uint32(event.NodeID),
				},
			},
		}
	default:
		panic("toProtoEvent: unknown event type")
	}
}
