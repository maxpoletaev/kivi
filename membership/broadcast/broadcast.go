package broadcast

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kv/gossip"
	"github.com/maxpoletaev/kv/membership"
)

type GossipBroadcaster struct {
	gossiper *gossip.Gossiper
}

func New(gossiper *gossip.Gossiper) *GossipBroadcaster {
	return &GossipBroadcaster{
		gossiper: gossiper,
	}
}

func (p *GossipBroadcaster) RegisterReceiver(member *membership.Member) error {
	_, err := p.gossiper.Register(gossip.PeerID(member.ID), member.GossipAddr)
	return err
}

func (p *GossipBroadcaster) UnregisterReceiver(member *membership.Member) {
	p.gossiper.Unregister(gossip.PeerID(member.ID))
}

func (p *GossipBroadcaster) Broadcast(event membership.ClusterEvent) error {
	b, err := protobuf.Marshal(membership.ToEventProto(event))
	if err != nil {
		return fmt.Errorf("failed to marshal cluster event: %w", err)
	}

	err = p.gossiper.Broadcast(b)
	if err != nil {
		return fmt.Errorf("failed to broadcast cluster event: %w", err)
	}

	return nil
}
