package clustering

import (
	"fmt"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kv/clustering/proto"
)

type GossipDelegate struct {
	events chan ClusterEvent
}

func NewGossipDelegate() *GossipDelegate {
	return &GossipDelegate{
		events: make(chan ClusterEvent),
	}
}

func (d *GossipDelegate) Receive([]byte) error {
	return nil
}

func (d *GossipDelegate) Deliver(b []byte) error {
	msg := &proto.ClusterEvent{}

	if err := protobuf.Unmarshal(b, msg); err != nil {
		return fmt.Errorf("failed to unmarshal cluster event: %w", err)
	}

	d.events <- FromProtoEvent(msg)

	return nil
}

func (d *GossipDelegate) Close() {
	close(d.events)
}

func (d *GossipDelegate) Events() <-chan ClusterEvent {
	return d.events
}
