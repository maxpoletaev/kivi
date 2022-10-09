package clustering

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kv/gossip"
)

type GossipEventPublisher struct {
	gossiper *gossip.Gossiper
}

func NewGossipEventPublisher(gossiper *gossip.Gossiper) *GossipEventPublisher {
	return &GossipEventPublisher{
		gossiper: gossiper,
	}
}

func (p *GossipEventPublisher) RegisterNode(node *Member) {
	p.gossiper.Register(gossip.PeerID(node.ID), node.GossipAddr)
}

func (p *GossipEventPublisher) UnregisterNode(node *Member) {
	p.gossiper.Unregister(gossip.PeerID(node.ID))
}

func (p *GossipEventPublisher) Publish(event ClusterEvent) error {
	b, err := protobuf.Marshal(ToProtoEvent(event))
	if err != nil {
		return fmt.Errorf("failed to marshal cluster event: %w", err)
	}

	err = p.gossiper.Broadcast(b)
	if err != nil {
		return fmt.Errorf("failed to broadcast cluster event: %w", err)
	}

	return nil
}
