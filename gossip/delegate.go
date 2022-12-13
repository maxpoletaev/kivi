package gossip

// Delegate is an interface the client should implement to receive gossip
// messages. The methods are never called concurrently and guaranteed to be
// called at most once for each message.
type Delegate interface {
	// Receive is called when a message is received by the consumer.
	// The order of calls is non-deterministic, since the messages may be
	// received in different order depending on the transport protocol.
	Receive([]byte) error

	// Deliver is called when a message is ready to be delivered. Which means
	// that and all its predecessor have already been delivered. It guarantees
	// that the messages broadcasted by the same node always
	// arrive in the same order.
	Deliver([]byte) error
}

// NoopDelegate is an event delegate that does nothing.
type NoopDelegate struct{}

func (d *NoopDelegate) Receive([]byte) error { return nil }
func (d *NoopDelegate) Deliver([]byte) error { return nil }

// Ensure NoopDeletgate satisfies the Delegate interface.
var _ Delegate = &NoopDelegate{}
