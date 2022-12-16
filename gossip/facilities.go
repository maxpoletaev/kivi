package gossip

import (
	"net/netip"

	"github.com/maxpoletaev/kv/gossip/proto"
)

// Transport is the underlying transport protocol used to deliver peer-to-peer
// messages from one node to another.
type Transport interface {
	// WriteTo writes a message to the network address.
	WriteTo(*proto.GossipMessage, *netip.AddrPort) error

	// ReadFrom reads message from the network and stores into the given message.
	// This will block until a message is received or the transport is closed.
	ReadFrom(*proto.GossipMessage) error

	// Close closes the underlying transport connection. After that, no messages
	// can be read or written to the transport and transport.ErrClosed is returned.
	Close() error
}
