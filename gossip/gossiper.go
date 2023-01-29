package gossip

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kiwi/gossip/proto"
	"github.com/maxpoletaev/kiwi/gossip/transport"
	"github.com/maxpoletaev/kiwi/internal/bloom"
	"github.com/maxpoletaev/kiwi/internal/generic"
	"github.com/maxpoletaev/kiwi/internal/rolling"
)

const (
	bloomFilterBits    = 128
	bloomFilterHashers = 3
)

// PeerID is a uinque 32-bit peer identifier.
type PeerID uint32

// Bytes returns the byte representation of the PeerID.
func (p PeerID) Bytes() []byte {
	return []byte{
		byte(0xff & p),
		byte(0xff & (p >> 8)),
		byte(0xff & (p >> 16)),
		byte(0xff & (p >> 24)),
	}
}

type remotePeer struct {
	ID      PeerID
	Addr    netip.AddrPort
	Queue   *MessageQueue
	Backlog *Backlog
	Timers  map[uint64]time.Time
	Blocked int
}

type peerMap map[PeerID]*remotePeer

// Gossiper is a peer-to-peer gossip protocol implementation. It is responsible
// for maintaining a list of known peers and exchanging messages with them. All
// received messages are passed to the delegate for processing.
type Gossiper struct {
	peerID              PeerID
	delegate            Delegate
	logger              log.Logger
	transport           Transport
	gossipFactor        int
	messageTTL          uint32
	lastSeqNum          *rolling.Counter[uint64]
	enableBF            bool
	wg                  sync.WaitGroup
	peersMut            sync.RWMutex
	peers               peerMap
	retransmitTimeout   time.Duration
	retransmitThreshold int
	backlogSize         int
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
	return &Gossiper{
		peerID:              conf.PeerID,
		gossipFactor:        conf.GossipFactor,
		transport:           conf.Transport,
		delegate:            conf.Delegate,
		logger:              conf.Logger,
		messageTTL:          conf.MessageTTL,
		lastSeqNum:          rolling.NewCounter[uint64](),
		enableBF:            conf.EnableBloomFilter,
		backlogSize:         conf.RetransmitBacklogSize,
		retransmitTimeout:   conf.RetransmitTimeout,
		retransmitThreshold: conf.RetransmitThreshold,
		peers:               make(peerMap),
	}
}

func (g *Gossiper) getPeers() peerMap {
	g.peersMut.RLock()
	peers := make(peerMap, len(g.peers))
	generic.MapCopy(g.peers, peers)
	g.peersMut.RUnlock()

	return peers
}

func (g *Gossiper) processRepeatReq(msg *proto.GossipMessage, req *proto.RepeatReq) {
	peers := g.getPeers()

	sender, ok := peers[PeerID(msg.PeerId)]
	if !ok || sender.ID == g.peerID {
		return
	}

	sourcePeer, ok := peers[PeerID(req.FromPeerId)]
	if !ok || sourcePeer.ID == g.peerID {
		return
	}

	missingIDs := make(map[uint64]struct{})

	for _, seqNum := range req.SeqNumbers {
		if payload, ok := sourcePeer.Backlog.Get(seqNum); ok {
			level.Debug(g.logger).Log(
				"msg", "retransmitting a message",
				"target_peer", sender.ID,
				"seqNum", seqNum,
			)

			msg := &proto.GossipMessage{
				PeerId: uint32(sourcePeer.ID),
				Message: &proto.GossipMessage_Payload{
					Payload: payload,
				},
				SeenBy: nil,
				Ttl:    0,
			}

			// The message is in the backlog, unicast it to the peer.
			if err := g.transport.WriteTo(msg, &sender.Addr); err != nil {
				level.Error(g.logger).Log("msg", "failed to send message", "err", err)
			}
		} else {
			// The messages that are not in the backlog will be requested from other peers.
			missingIDs[seqNum] = struct{}{}
		}
	}

	if len(msg.SeenBy) > 0 {
		bf := bloom.New(msg.SeenBy, bloomFilterHashers)
		bf.Add(g.peerID.Bytes())
	}

	// If there are still missing messages, request them from other peers.
	// We reconstruct the message with the updated TTL, SeqNumbers and SeenBy fields.
	if len(missingIDs) > 0 && msg.Ttl > 0 {
		newmsg := &proto.GossipMessage{
			PeerId: msg.PeerId,
			Ttl:    msg.Ttl - 1,
			SeenBy: msg.SeenBy,
			Message: &proto.GossipMessage_RepeatReq{
				RepeatReq: &proto.RepeatReq{
					FromPeerId: req.FromPeerId,
					SeqNumbers: generic.MapKeys(missingIDs),
				},
			},
		}

		g.wg.Add(1)

		go func() {
			defer g.wg.Done()

			if err := g.gossip(newmsg); err != nil {
				level.Error(g.logger).Log("msg", "failed to broadcast message", "err", err)
			}
		}()
	}
}

func (g *Gossiper) processDataMessage(msg *proto.GossipMessage, pl *proto.Payload) {
	l := log.WithSuffix(g.logger, "from_peer", msg.PeerId, "seq_num", pl.SeqNumber)

	if msg.Ttl > 0 {
		level.Debug(l).Log("msg", "scheduled for rebroadcast", "ttl", msg.Ttl)

		if len(msg.SeenBy) > 0 {
			// Keep track of peers that have seen this message.
			bf := bloom.New(msg.SeenBy, bloomFilterHashers)
			bf.Add(g.peerID.Bytes())
		}

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

	knownPeers := g.getPeers()

	peer, ok := knownPeers[peerID]
	if !ok {
		level.Warn(l).Log("msg", "got message from an unknown peer")
		return
	}

	// Put the message in the backlog, so that we can retransmit it when requested.
	peer.Backlog.Add(pl)

	// Delete the retransmit timer since we got the message.
	if _, ok := peer.Timers[pl.SeqNumber]; ok {
		level.Debug(l).Log("msg", "retransmit recevied", "seq", pl.SeqNumber)
		delete(peer.Timers, pl.SeqNumber)
	}

	if peer.Queue.Push(pl) {
		level.Debug(l).Log("msg", "message is added to the queue", "queue_len", peer.Queue.Len())

		if err := g.delegate.Receive(pl.Data); err != nil {
			level.Error(l).Log("msg", "message receive failed", "err", err)
		}
	}

	for {
		var (
			nextSeqNum = peer.Queue.NextSeqNum()
			next       = peer.Queue.PopNext()
		)

		if next == nil {
			// First, try unblocking the queue by skipping the next message if,
			// we have already been waiting for the retransmission for too long.
			if started, ok := peer.Timers[nextSeqNum]; ok {
				if time.Since(started) > g.retransmitTimeout {
					level.Warn(l).Log("msg", "message skipped", "seq_num", nextSeqNum)
					delete(peer.Timers, nextSeqNum)
					peer.Queue.SkipTo(nextSeqNum)

					continue
				}
			}

			// If there is nothing to skip and the queue is still blocked, we increment the
			// blocked counter and wait for the next message, that might unblock the queue.
			if peer.Queue.Len() > 0 {
				peer.Blocked++
			}

			level.Debug(l).Log(
				"msg", "no more messages available",
				"blocked_count", peer.Blocked,
				"queue_len", peer.Queue.Len(),
			)

			break
		}

		if err := g.delegate.Deliver(next.Data); err != nil {
			level.Error(l).Log("msg", "message delivery failed", "err", err)
			continue
		}

		level.Debug(l).Log("msg", "message delivered", "queue_len", peer.Queue.Len())

		peer.Blocked = 0

		return
	}

	// Once the delivery from the peer is blocked more than a configured threshold, we ask other
	// peers for retransmission of missing messages. This is done only once per peer, so that we
	// don't spam the network with retransmission requests.
	if peer.Blocked >= g.retransmitThreshold {
		if err := g.askRetransmit(peer); err != nil {
			level.Warn(l).Log("msg", "failed to ask for retransmission", "err", err)
		}
	}
}

func (g *Gossiper) processMessage(msg *proto.GossipMessage) {
	switch msg.Message.(type) {
	case *proto.GossipMessage_Payload:
		g.processDataMessage(msg, msg.GetPayload())
	case *proto.GossipMessage_RepeatReq:
		g.processRepeatReq(msg, msg.GetRepeatReq())
	default:
		level.Error(g.logger).Log("msg", "unknown message type", "type", fmt.Sprintf("%T", msg.Message))
	}
}

func (g *Gossiper) askRetransmit(peer *remotePeer) error {
	var (
		gaps           = peer.Queue.FindGaps()
		missingSeqNums = make([]uint64, 0, len(gaps))
	)

	for _, seq := range peer.Queue.FindGaps() {
		if _, ok := peer.Timers[seq]; ok {
			continue // We have already asked and currently waiting, for this message.
		}

		missingSeqNums = append(missingSeqNums, seq)
	}

	if len(missingSeqNums) > 0 {
		level.Debug(g.logger).Log(
			"msg", "asking for retransmission",
			"source_peer", peer.ID,
			"seq_nums", fmt.Sprintf("%v", missingSeqNums),
		)

		var (
			now    = time.Now()
			seenBy []byte
		)

		// Setup retransmit timers for the missing messages. If we don't get
		// the messages within the timeout, we consider the message as lost.
		for _, seq := range missingSeqNums {
			peer.Timers[seq] = now
		}

		if g.enableBF {
			seenBy = make([]byte, bloomFilterBits/8+1)
			bf := bloom.New(seenBy, bloomFilterHashers)
			bf.Add(g.peerID.Bytes())
		}

		msg := &proto.GossipMessage{
			PeerId: uint32(g.peerID),
			Ttl:    g.initialTTL(),
			SeenBy: seenBy,
			Message: &proto.GossipMessage_RepeatReq{
				RepeatReq: &proto.RepeatReq{
					FromPeerId: uint32(peer.ID),
					SeqNumbers: missingSeqNums,
				},
			},
		}

		return g.gossip(msg)
	}

	return nil
}

func (g *Gossiper) gossip(msg *proto.GossipMessage) error {
	var (
		seenBy      *bloom.Filter
		lastErr     error
		sentCount   int
		failedCount int
	)

	knownPeers := g.getPeers()
	peerIDs := generic.MapKeys(knownPeers)

	generic.Shuffle(peerIDs)

	if len(msg.SeenBy) > 0 {
		seenBy = bloom.New(msg.SeenBy, bloomFilterHashers)
	}

	for _, id := range peerIDs {
		if sentCount >= g.gossipFactor {
			break
		}

		peer := knownPeers[id]

		// Skip peers that have already seen this message.
		if seenBy != nil && seenBy.Check(peer.ID.Bytes()) {
			continue
		}

		sentCount++

		err := g.transport.WriteTo(msg, &peer.Addr)
		if err != nil {
			level.Error(g.logger).Log("msg", "failed to sent a message", "to", peer.Addr)

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

	g.peersMut.RLock()
	peerCount := len(g.peers)
	g.peersMut.RUnlock()

	return autoTTL(peerCount, g.gossipFactor)
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

// AddPeer adds new peer for broadcasting messages to.
func (g *Gossiper) AddPeer(id PeerID, addr string) (bool, error) {
	addrPort, err := netip.ParseAddrPort(addr)
	if err != nil {
		return false, fmt.Errorf("failed to parse peer address: %w", err)
	}

	g.peersMut.Lock()
	defer g.peersMut.Unlock()

	if _, ok := g.peers[id]; ok {
		return false, nil
	}

	g.peers[id] = &remotePeer{
		ID:      id,
		Addr:    addrPort,
		Queue:   NewQueue(),
		Backlog: NewBacklog(g.backlogSize),
		Timers:  make(map[uint64]time.Time),
	}

	level.Debug(g.logger).Log("msg", "new peer registered", "id", id, "addr", addr)

	return true, nil
}

// RemovePeer removes peer from the list of known peers.
func (g *Gossiper) RemovePeer(id PeerID) bool {
	g.peersMut.Lock()
	defer g.peersMut.Unlock()

	if _, ok := g.peers[id]; !ok {
		return false
	}

	delete(g.peers, id)

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
		PeerId: uint32(g.peerID),
		Ttl:    g.initialTTL(),
		Message: &proto.GossipMessage_Payload{
			Payload: &proto.Payload{
				SeqNumber:   seqNumber,
				SeqRollover: rollover,
				Data:        payload,
			},
		},
	}

	if g.enableBF {
		msg.SeenBy = make([]byte, (bloomFilterBits/8)+1)
		bf := bloom.New(msg.SeenBy, bloomFilterHashers)
		bf.Add(g.peerID.Bytes())
	}

	return g.gossip(msg)
}

// autoTTL returns optimal TTL for a message to reach all nodes.
func autoTTL(nPeers, gossipFactor int) uint32 {
	return uint32(math.Log(float64(nPeers))/math.Log(float64(gossipFactor))) + 1
}
