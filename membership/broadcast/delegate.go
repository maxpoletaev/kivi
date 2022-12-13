package broadcast

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
)

type GossipEventDelegate struct {
	events chan membership.ClusterEvent
}

func NewGossipEventDelegate() *GossipEventDelegate {
	return &GossipEventDelegate{
		events: make(chan membership.ClusterEvent),
	}
}

func (d *GossipEventDelegate) Receive([]byte) error {
	return nil
}

func (d *GossipEventDelegate) Deliver(b []byte) error {
	msg := &proto.ClusterEvent{}

	if err := protobuf.Unmarshal(b, msg); err != nil {
		return fmt.Errorf("failed to unmarshal cluster event: %w", err)
	}

	d.events <- membership.FromEventProto(msg)

	return nil
}

func (d *GossipEventDelegate) Close() {
	close(d.events)
}

func (d *GossipEventDelegate) Events() <-chan membership.ClusterEvent {
	return d.events
}
