package broadcast

import (
	"fmt"

	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/membership/proto"
	protobuf "google.golang.org/protobuf/proto"
)

type EventReceiver struct {
	ch chan membership.ClusterEvent
}

func NewReceiver() *EventReceiver {
	return &EventReceiver{
		ch: make(chan membership.ClusterEvent),
	}
}

func (d *EventReceiver) Receive(b []byte) error {
	return nil
}

func (d *EventReceiver) Deliver(b []byte) error {
	msg := &proto.ClusterEvent{}

	if err := protobuf.Unmarshal(b, msg); err != nil {
		return fmt.Errorf("failed to unmarshal cluster event: %w", err)
	}

	event := fromProtoEvent(msg)

	d.ch <- event

	return nil
}

func (d *EventReceiver) Chan() <-chan membership.ClusterEvent {
	return d.ch
}

func (d *EventReceiver) Close() {
	close(d.ch)
}
