package gossip

import (
	"errors"
	"fmt"
	"math"
	"net"
	"sync/atomic"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"golang.org/x/sync/errgroup"

	"github.com/maxpoletaev/kv/gossip/proto"
	"github.com/maxpoletaev/kv/gossip/queue"
	"github.com/maxpoletaev/kv/gossip/transport"
	"github.com/maxpoletaev/kv/internal/generics"
)

// Delegate is an interface the client should implement to receive gossip
// messages.The methods are never called concurrently and guaranteed to be
// called only once for each message.
type Delegate interface {
	// Receive is called when a message is received by the consumer.
	// The order of calls is non-deterministic, since the messages may be
	// received in different order depending on the transport protocol.
	Receive([]byte)

	// Deliver is called when a message is ready to be delivered. Which means
	// that and all its predecessor have already been delivered. It guarantees
	// that the messages broadcasted by the same node always
	// arrive in the same order.
	Deliver([]byte)
}

// Transport is the underlying transport protocol used
// to deliver peer-to-peer messages from one node to another.
type Transport interface {
	WriteTo(*proto.GossipMessage, transport.Addressable) error
	ReadFrom(*proto.GossipMessage) error
	Close() error
}

// Addr is a protocol-agnostic network address.
type Addr struct {
	IP   net.IP
	Port int
}

func (p *Addr) String() string {
	return fmt.Sprintf("%s:%d", p.IP, p.Port)
}

func (p *Addr) UDPAddr() *net.UDPAddr {
	return &net.UDPAddr{
		IP:   p.IP,
		Port: p.Port,
	}
}

// PeerID is a uinque 32-bit peer identifier.
type PeerID uint32

// peerNode represents a remote peer node.
type peerNode struct {
	ID    PeerID
	Addr  *Addr
	Queue *queue.OrderedQueue
}

type peerMap map[PeerID]*peerNode

type Gossiper struct {
	peerID    PeerID
	delegate  Delegate
	logger    log.Logger
	transport Transport

	gossipFactor int
	messageTTL   uint32
	lastSeqNum   uint64
	done         chan struct{}

	// Peers is the registry of known peers, to which messages will be gossiped.
	// Implemented as a map wrapped into atomic.Value to provide lock-free reads.
	peers *generics.AtomicValue[peerMap]
}

// Start initializes the gossiper struct with the given configuration
// and starts a background listener process accepting gossip messages.
func Start(conf *Config) (*Gossiper, error) {
	c := &Config{}
	*c = *conf

	if c.Transport == nil {
		tr, err := transport.Create(c.BindAddr.UDPAddr())
		if err != nil {
			return nil, err
		}

		tr.Logger = c.Logger
		c.Transport = tr

		go tr.Consume()
	}

	g := newGossiper(c)
	go g.listenMessages()

	return g, nil
}

func newGossiper(cfg *Config) *Gossiper {
	peers := &generics.AtomicValue[peerMap]{}
	peers.Store(make(peerMap))

	return &Gossiper{
		peerID:       cfg.PeerID,
		gossipFactor: cfg.GossipFactor,
		transport:    cfg.Transport,
		delegate:     cfg.Delegate,
		logger:       cfg.Logger,
		messageTTL:   cfg.MessageTTL,
		done:         make(chan struct{}),
		peers:        peers,
	}
}

func (g *Gossiper) processMessage(msg *proto.GossipMessage) {
	l := log.WithSuffix(g.logger, "from_peer", msg.PeerId, "seq_num", msg.SeqNumber)

	if msg.Ttl > 0 {
		level.Debug(l).Log("msg", "scheduled for rebroadcast", "ttl", msg.Ttl)

		msg.SeenBy = append(msg.SeenBy, uint32(g.peerID))
		msg.Ttl--

		go func() {
			if err := g.gossip(msg); err != nil {
				level.Warn(l).Log("msg", "rebroadcast failed", "err", err)
			}
		}()
	}

	peerID := PeerID(msg.PeerId)
	if peerID == g.peerID {
		return // ignore our own messages
	}

	knownPeers := g.peers.Load()
	peer, ok := knownPeers[peerID]
	if !ok {
		level.Warn(l).Log("msg", "got message from an unknown peer")
		return
	}

	if peer.Queue.Push(msg) {
		level.Debug(l).Log("msg", "message is added to the queue", "queue_len", peer.Queue.Len())
		g.delegate.Receive(msg.Payload)
	}

	for {
		next := peer.Queue.PopNext()
		if next == nil {
			level.Debug(l).Log("msg", "no more messages available", "queue_len", peer.Queue.Len())
			break
		}

		g.delegate.Deliver(next.Payload)

		level.Debug(l).Log("msg", "message delivered", "queue_len", peer.Queue.Len())
	}
}

func (g *Gossiper) gossip(msg *proto.GossipMessage) error {
	seenBy := make(map[PeerID]bool, len(msg.SeenBy))
	for _, peerID := range msg.SeenBy {
		seenBy[PeerID(peerID)] = true
	}

	var scheduled int
	var errg errgroup.Group

	knownPeers := g.peers.Load()

	for id := range knownPeers {
		if scheduled >= g.gossipFactor {
			break
		}

		peer := knownPeers[id]
		if seenBy[peer.ID] {
			continue
		}

		errg.Go(func() error {
			err := g.transport.WriteTo(msg, peer.Addr)
			if err != nil {
				return err
			}

			return nil
		})

		scheduled++
	}

	return errg.Wait()
}

func (g *Gossiper) initialTTL() uint32 {
	if g.messageTTL > 0 {
		return g.messageTTL
	}

	peers := g.peers.Load()

	return autoTTL(len(peers), g.gossipFactor)
}

func (g *Gossiper) listenMessages() {
	level.Debug(g.logger).Log("msg", "gossip listener started", "peer_id", g.peerID)

	for {
		msg := &proto.GossipMessage{}

		if err := g.transport.ReadFrom(msg); err != nil {
			if errors.Is(err, transport.ErrClosed) {
				level.Warn(g.logger).Log("msg", "transport is closed, shutting down")
				break
			}

			level.Error(g.logger).Log("msg", "error while reading", "err", err)

			continue
		}

		level.Debug(g.logger).Log(
			"msg", "received gossip message",
			"from", msg.PeerId,
			"seq", msg.SeqNumber,
			"ttl", msg.Ttl,
		)

		g.processMessage(msg)
	}

	close(g.done)
}

// Shutdown stops the gossiper and waits until the last received message is processed.
// Once stopped, it cannot be started again.
func (g *Gossiper) Shutdown() error {
	if err := g.transport.Close(); err != nil {
		return fmt.Errorf("failed to close transport: %w", err)
	}

	<-g.done

	return nil
}

// Register adds new peer for broadcasting messages to.
func (g *Gossiper) Register(id PeerID, addr *Addr) bool {
	g.peers.Lock()
	defer g.peers.Unlock()

	oldMap := g.peers.Load()
	if _, ok := oldMap[id]; ok {
		return false
	}

	newMap := make(peerMap, len(oldMap)+1)
	generics.MapCopy(oldMap, newMap)

	newMap[id] = &peerNode{
		ID:    id,
		Addr:  addr,
		Queue: queue.New(),
	}

	g.peers.Store(newMap)

	return true
}

// Unregister removes peer from the list of known peers.
func (g *Gossiper) Unregister(id PeerID) bool {
	g.peers.Lock()
	defer g.peers.Unlock()

	oldMap := g.peers.Load()
	if _, ok := oldMap[id]; !ok {
		return false
	}

	newMap := make(peerMap, len(oldMap)-1)
	for key, value := range oldMap {
		if key == id {
			continue
		}

		newMap[key] = value
	}

	g.peers.Store(newMap)

	return true
}

// Broadcast sends the given data to all nodes through the gossip network.
// For UDP, the size of the payload should not exceed the MTU size (which is
// typically 1500 bytes in most networks). However, when working in less
// predictable environments, keeping the message size within 512 bytes
// is recommended to avoid packet fragmentation.
func (g *Gossiper) Broadcast(payload []byte) error {
	seqNumber := atomic.AddUint64(&g.lastSeqNum, 1)

	msg := &proto.GossipMessage{
		PeerId:    uint32(g.peerID),
		Ttl:       g.initialTTL(),
		SeqNumber: seqNumber,
		Payload:   payload,
	}

	return g.gossip(msg)
}

// autoTTL returns optimal TTL for a message to reach all nodes.
func autoTTL(nPeers, gossipFactor int) uint32 {
	return uint32(math.Log(float64(nPeers))/math.Log(float64(gossipFactor))) + 1
}
