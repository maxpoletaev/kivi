package transport

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kv/gossip/proto"
)

const (
	maxPayloadSize    = 1500 // implied by MTU
	receiveBufferSize = 1 * 1024 * 1024
)

var (
	ErrClosed          = errors.New("connection closed")
	ErrMaxSizeExceeded = errors.New("max payload size exceeded")
)

type Addressable interface {
	UDPAddr() *net.UDPAddr
}

type packet struct {
	len  int
	body []byte
	from net.Addr
}

func (p *packet) Body() []byte {
	return p.body[:p.len]
}

type UDPTransport struct {
	Logger log.Logger

	packetPool *sync.Pool
	conn       *net.UDPConn
	received   chan *packet
	done       chan struct{}
	closed     int32
}

// Create starts a UDP listener on the given address.
func Create(bindAddr *netip.AddrPort) (*UDPTransport, error) {
	conn, err := net.ListenUDP("udp", asUDPAddr(bindAddr))
	if err != nil {
		return nil, fmt.Errorf("failed to listen udp port on %s: %w", bindAddr, err)
	}

	// Set system buffer to larger size to reduce the number of packet drops
	// when the consumer is too busy to keep up with the incoming message rate.
	if err := conn.SetReadBuffer(receiveBufferSize); err != nil {
		return nil, fmt.Errorf("failed to alter udp read buffer size: %w", err)
	}

	pool := &sync.Pool{
		New: func() any {
			return &packet{
				body: make([]byte, maxPayloadSize),
			}
		},
	}

	t := &UDPTransport{
		conn:       conn,
		packetPool: pool,
		received:   make(chan *packet),
		done:       make(chan struct{}),
	}

	go t.runLoop()

	return t, nil
}

func (t *UDPTransport) runLoop() {
	const (
		initialDelay = 30 * time.Millisecond
		maxDelay     = 10 * time.Second
	)

	delay := initialDelay

	for {
		dgram := t.packetPool.Get().(*packet)

		n, addr, err := t.conn.ReadFromUDP(dgram.body)

		if err != nil {
			if atomic.LoadInt32(&t.closed) == 1 {
				break
			}

			level.Error(t.Logger).Log("msg", "failed to read from udp", "err", err)

			t.packetPool.Put(dgram)

			time.Sleep(delay)

			delay *= delay
			if delay > maxDelay {
				delay = maxDelay
			}

			continue
		}

		delay = initialDelay

		if n == 0 {
			level.Warn(t.Logger).Log("msg", "received empty udp packet", "from", addr)
			t.packetPool.Put(dgram)
			continue
		}

		dgram.len = n
		dgram.from = addr

		t.received <- dgram
	}

	close(t.received)

	close(t.done)
}

// Close stops the consumer and closes the underlying UDP socket. Once closed,
// the transport cannot be reused. It blocks until the last read has completed.
// It is safe to call Close multiple times.
func (t *UDPTransport) Close() error {
	if atomic.CompareAndSwapInt32(&t.closed, 0, 1) {
		if err := t.conn.Close(); err != nil {
			return err
		}
	}

	<-t.done

	return nil
}

func (t *UDPTransport) ReadFrom(msg *proto.GossipMessage) error {
	dgram := <-t.received
	if dgram == nil {
		return ErrClosed
	}

	defer t.packetPool.Put(dgram)

	if err := protobuf.Unmarshal(dgram.Body(), msg); err != nil {
		return fmt.Errorf("failed to unmarshal gossip message: %w", err)
	}

	return nil
}

func (t *UDPTransport) WriteTo(msg *proto.GossipMessage, addr *netip.AddrPort) error {
	payload, err := protobuf.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal gossip message: %w", err)
	}

	if len(payload) > maxPayloadSize {
		return ErrMaxSizeExceeded
	}

	if _, err = t.conn.WriteToUDP(payload, asUDPAddr(addr)); err != nil {
		if atomic.LoadInt32(&t.closed) == 1 {
			return ErrClosed
		}

		return fmt.Errorf("failed to send message to upd socket: %w", err)
	}

	return nil
}

func asUDPAddr(addr *netip.AddrPort) *net.UDPAddr {
	return &net.UDPAddr{
		IP:   addr.Addr().AsSlice(),
		Port: int(addr.Port()),
	}
}
