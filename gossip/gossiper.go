package gossip

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kv/gossip/proto"
	"github.com/maxpoletaev/kv/gossip/queue"
	"github.com/maxpoletaev/kv/gossip/transport"
	"github.com/maxpoletaev/kv/internal/generic"
	"github.com/maxpoletaev/kv/internal/rolling"
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

// PeerID is a uinque 32-bit peer identifier.
type PeerID uint32

// remotePeer represents a remote peer node.
type remotePeer struct {
	ID    PeerID
	Addr  netip.AddrPort
	Queue *queue.OrderedQueue
}

type peerMap map[PeerID]*remotePeer

type Gossiper struct {
	peerID    PeerID
	delegate  Delegate
	logger    log.Logger
	transport Transport

	gossipFactor int
	messageTTL   uint32
	lastSeqNum   *rolling.Counter[uint64]
	wg           sync.WaitGroup

	// Peers is the registry of known peers, to which messages will be gossiped.
	// Implemented as a map wrapped into atomic.Value to provide lock-free reads.
	peers *generic.Atomic[peerMap]
}

// Start initializes the gossiper struct with the given configuration
// and starts a background listener process accepting gossip messages.
func Start(conf *Config) (*Gossiper, error) {
	bindAddr, err := netip.ParseAddrPort(conf.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bind address (%s): %w", conf.BindAddr, err)
	}

	if conf.Transport == nil {
		// Ensure that the original config is not modified.
		conf = func() *Config { c := *conf; return &c }()

		tr, err := transport.Create(&bindAddr)
		if err != nil {
			return nil, err
		}

		tr.Logger = conf.Logger

		conf.Transport = tr
	}

	g := newGossiper(conf)

	g.StartListener()

	return g, nil
}

func newGossiper(conf *Config) *Gossiper {
	peers := &generic.Atomic[peerMap]{}
	peers.Store(make(peerMap))

	return &Gossiper{
		peerID:       conf.PeerID,
		gossipFactor: conf.GossipFactor,
		transport:    conf.Transport,
		delegate:     conf.Delegate,
		logger:       conf.Logger,
		messageTTL:   conf.MessageTTL,
		lastSeqNum:   rolling.NewCounter[uint64](),
		peers:        peers,
	}
}

func (g *Gossiper) processMessage(msg *proto.GossipMessage) {
	l := log.WithSuffix(g.logger, "from_peer", msg.PeerId, "seq_num", msg.SeqNumber)

	if msg.Ttl > 0 {
		level.Debug(l).Log("msg", "scheduled for rebroadcast", "ttl", msg.Ttl)

		// Keep track of peers that have seen the message in order to avoid recursion.
		// TODO: probably should make it configurable as it may affect message size
		//	on large ttls. Or maybe use bloom filter.
		msg.SeenBy = append(msg.SeenBy, uint32(g.peerID))

		msg.Ttl--

		g.wg.Add(1)

		go func() {
			defer g.wg.Done()

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

		if err := g.delegate.Receive(msg.Payload); err != nil {
			level.Error(l).Log("msg", "message receive failed", "err", err)
		}
	}

	for {
		next := peer.Queue.PopNext()
		if next == nil {
			level.Debug(l).Log("msg", "no more messages available", "queue_len", peer.Queue.Len())
			break
		}

		if err := g.delegate.Deliver(next.Payload); err != nil {
			level.Error(l).Log("msg", "message delivery failed", "err", err)
			continue
		}

		level.Debug(l).Log("msg", "message delivered", "queue_len", peer.Queue.Len())
	}
}

func (g *Gossiper) gossip(msg *proto.GossipMessage) error {
	seenBy := make(map[PeerID]bool, len(msg.SeenBy))
	for _, peerID := range msg.SeenBy {
		seenBy[PeerID(peerID)] = true
	}

	var lastErr error

	var sentCount, failedCount int

	knownPeers := g.peers.Load()

	peerIDs := generic.MapKeys(knownPeers)

	generic.Shuffle(peerIDs)

	for _, id := range peerIDs {
		if sentCount >= g.gossipFactor {
			break
		}

		p := knownPeers[id]
		if seenBy[p.ID] {
			continue
		}

		sentCount++

		err := g.transport.WriteTo(msg, &p.Addr)
		if err != nil {
			level.Error(g.logger).Log("msg", "failed to sent a message", "to", p.Addr)

			if lastErr == nil {
				lastErr = err
			}

			failedCount++
		}
	}

	// Error only if all attempts have failed.
	if failedCount > 0 && sentCount == failedCount {
		return lastErr
	}

	return nil
}

func (g *Gossiper) initialTTL() uint32 {
	if g.messageTTL > 0 {
		return g.messageTTL
	}

	peers := g.peers.Load()

	return autoTTL(len(peers), g.gossipFactor)
}

// StartListener starts the background listener process.
func (g *Gossiper) StartListener() {
	g.wg.Add(1)

	go func() {
		g.listenMessages()
		g.wg.Done()
	}()
}

func (g *Gossiper) listenMessages() {
	level.Debug(g.logger).Log("msg", "gossip listener started", "peer_id", g.peerID)

	for {
		msg := &proto.GossipMessage{}

		if err := g.transport.ReadFrom(msg); err != nil {
			if errors.Is(err, transport.ErrClosed) {
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
}

// Shutdown stops the gossiper and waits until the last received message
// is processed. Once stopped, it cannot be started again.
func (g *Gossiper) Shutdown() error {
	if err := g.transport.Close(); err != nil {
		return fmt.Errorf("failed to close transport: %w", err)
	}

	g.wg.Wait()

	return nil
}

// Register adds new peer for broadcasting messages to.
func (g *Gossiper) Register(id PeerID, addr string) (bool, error) {
	g.peers.Lock()
	defer g.peers.Unlock()

	addrPort, err := netip.ParseAddrPort(addr)
	if err != nil {
		return false, fmt.Errorf("failed to parse peer address: %w", err)
	}

	oldMap := g.peers.Load()
	if _, ok := oldMap[id]; ok {
		return false, nil
	}

	newMap := make(peerMap, len(oldMap)+1)

	generic.MapCopy(oldMap, newMap)

	newMap[id] = &remotePeer{
		ID:    id,
		Addr:  addrPort,
		Queue: queue.New(),
	}

	g.peers.Store(newMap)

	level.Debug(g.logger).Log("msg", "new peer registered", "id", id, "addr", addr)

	return true, nil
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
	for k := range oldMap {
		if k != id {
			newMap[k] = oldMap[k]
		}
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
	seqNumber, rollover := g.lastSeqNum.Inc()

	msg := &proto.GossipMessage{
		PeerId:      uint32(g.peerID),
		Ttl:         g.initialTTL(),
		SeqNumber:   seqNumber,
		SeqRollover: rollover,
		Payload:     payload,
	}

	return g.gossip(msg)
}

// autoTTL returns optimal TTL for a message to reach all nodes.
func autoTTL(nPeers, gossipFactor int) uint32 {
	return uint32(math.Log(float64(nPeers))/math.Log(float64(gossipFactor))) + 1
}
