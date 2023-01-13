package broadcast

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kiwi/gossip"
	"github.com/maxpoletaev/kiwi/membership"
)

type EventSender struct {
	gossiper *gossip.Gossiper
}

func NewSender(gossiper *gossip.Gossiper) *EventSender {
	return &EventSender{
		gossiper: gossiper,
	}
}

func (p *EventSender) RegisterReceiver(member *membership.Member) error {
	_, err := p.gossiper.AddPeer(gossip.PeerID(member.RandID), member.GossipAddr)
	return err
}

func (p *EventSender) UnregisterReceiver(member *membership.Member) {
	p.gossiper.RemovePeer(gossip.PeerID(member.RandID))
}

func (p *EventSender) Broadcast(event membership.ClusterEvent) error {
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
