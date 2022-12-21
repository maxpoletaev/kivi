package gossip

import (
	"github.com/go-kit/log"
)

type Config struct {
	// PeerID identifier of the current node. It will be attached to every
	// message and must be be unique.
	PeerID PeerID

	// GossipFactor is the number of nodes a message is broadcasted to.
	// The default value is 2. Higher values would increase protocol stability
	// but also increase network load.
	GossipFactor int

	// BindAddr is where gossiper will be expecting messages to appear. Other
	// nodes should know this address in order to broadcast messages to this node.
	BindAddr string

	// Delegate is the interface the user should implement in order to receive
	// broadcasted messages. See Delegate interface documentation for more detail.
	Delegate Delegate

	// Transport is the underlying transport protocol used to deliver peer-to-peer
	// messages from one node to another. If not defined, the default UDPTransport
	// is used.
	Transport Transport

	// MessageTTL is the number of times the message is passed from one node
	// to another. If not defined, the TTL will be set automatically. Same
	// as for GossipFactor, changing this value may affect protocol stability
	// and network load.
	MessageTTL uint32

	// Logger is go-kit logger used to record debug messages and non-critical
	// errors while protocol execution. If not provided, it will be totally silent.
	Logger log.Logger

	// BloomFilterK is the number of hash functions used in the bloom filter,
	// which is used to filter out redundant messages. The default value is 3.
	BloomFilterK int
}

// DefaultConfig creates a Config with reasonable default values
// that will not crash the program straight away.
func DefaultConfig() *Config {
	return &Config{
		GossipFactor: 2,
		BindAddr:     "0.0.0.0:1984",
		Logger:       log.NewNopLogger(),
		Delegate:     &NoopDelegate{},
		BloomFilterK: 3,
	}
}
