package gossip

import (
	"time"

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
	// errors while protocol execution. If not provided, no logs are written.
	Logger log.Logger

	// EnableBloomFilter enables bloom filter to reduce the number of redundant
	// messages received by the node. It is enabled by default.
	EnableBloomFilter bool

	// RetransmitBacklogSize is the maximum number of messages that is storead for
	// each peer. The backlog is kept in case other peers ask for retransmission.
	// The default value is 100.
	RetransmitBacklogSize int

	// RetransmitThreshold is the number of unsuccessful delivery attempts
	// in a row before the node starts asking other peers for retransmission.
	RetransmitThreshold int

	// RetransmitTimeout is the time after which, if no retransmission is received,
	// the message is considered completely lost. Once the message is lost, it will
	// be skipped to unblock the queue. In this case data loss is possible.
	// The default value is 10 seconds.
	RetransmitTimeout time.Duration
}

// DefaultConfig creates a Config with reasonable default values
// that will not crash the program straight away.
func DefaultConfig() *Config {
	return &Config{
		GossipFactor:          2,
		BindAddr:              "0.0.0.0:1984",
		Logger:                log.NewNopLogger(),
		Delegate:              &NoopDelegate{},
		EnableBloomFilter:     true,
		RetransmitBacklogSize: 100,
		RetransmitThreshold:   10,
		RetransmitTimeout:     10 * time.Second,
	}
}
