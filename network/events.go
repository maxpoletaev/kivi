package network

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kv/gossip"
	"github.com/maxpoletaev/kv/network/proto"
)

type ClusterEvent interface {
	isClusterEvent()
}

type NodeJoined struct {
	NodeID     NodeID
	NodeName   string
	GossipAddr string
	ServerAddr string
}

func (*NodeJoined) isClusterEvent() {}

type NodeLeft struct {
	NodeID NodeID
}

func (*NodeLeft) isClusterEvent() {}

// Ensure event types stisfy the interface.
var _ ClusterEvent = &NodeJoined{}
var _ ClusterEvent = &NodeLeft{}

type GossipEventPubSub struct {
	gossiper *gossip.Gossiper
}

func NewGossipEventPubSub(gossiper *gossip.Gossiper) *GossipEventPubSub {
	return &GossipEventPubSub{
		gossiper: gossiper,
	}
}

func (p *GossipEventPubSub) Subscribe(nodeID NodeID, addr string) {
	p.gossiper.Register(gossip.PeerID(nodeID), addr)
}

func (p *GossipEventPubSub) Unsubscribe(nodeID NodeID) {
	p.gossiper.Unregister(gossip.PeerID(nodeID))
}

func (p *GossipEventPubSub) Publish(event ClusterEvent) error {
	b, err := protobuf.Marshal(toProtoEvent(event))
	if err != nil {
		return fmt.Errorf("failed to marshal cluster event: %w", err)
	}

	err = p.gossiper.Broadcast(b)
	if err != nil {
		return fmt.Errorf("failed to broadcast cluster event: %w", err)
	}

	return nil
}

func fromProtoEvent(pe *proto.ClusterEvent) ClusterEvent {
	switch event := pe.Event.(type) {
	case *proto.ClusterEvent_Joined:
		node := event.Joined.Node

		return &NodeJoined{
			NodeID:     NodeID(node.Id),
			NodeName:   node.Name,
			ServerAddr: node.ServerAddr,
			GossipAddr: node.GossipAddr,
		}
	case *proto.ClusterEvent_Left:
		return &NodeLeft{
			NodeID: NodeID(event.Left.NodeId),
		}
	default:
		panic("fromProtoEvent: unknown event type")
	}
}

func toProtoEvent(e ClusterEvent) *proto.ClusterEvent {
	switch event := e.(type) {
	case *NodeJoined:
		return &proto.ClusterEvent{
			Event: &proto.ClusterEvent_Joined{
				Joined: &proto.NodeJoinedEvent{
					Node: &proto.Node{
						Id:         uint32(event.NodeID),
						Name:       event.NodeName,
						GossipAddr: event.GossipAddr,
						ServerAddr: event.ServerAddr,
					},
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
