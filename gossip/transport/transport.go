package transport

import (
	"errors"
	"fmt"
	"net"
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

	conn   *net.UDPConn
	pool   *sync.Pool
	in     chan *packet
	done   chan struct{}
	closed int32
}

// Create starts a UDP listener on the given address.
func Create(addr *net.UDPAddr) (*UDPTransport, error) {
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen udp port on %s: %w", addr, err)
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
		conn: conn,
		pool: pool,
		in:   make(chan *packet),
		done: make(chan struct{}),
	}

	return t, nil
}

func (t *UDPTransport) Consume() {
	const (
		initialDelay = 30 * time.Millisecond
		maxDelay     = 10 * time.Second
	)

	delay := initialDelay

	for {
		pkt := t.pool.Get().(*packet)

		n, addr, err := t.conn.ReadFromUDP(pkt.body)

		if err != nil {
			if atomic.LoadInt32(&t.closed) == 1 {
				break
			}

			level.Error(t.Logger).Log("msg", "failed to read from udp", "err", err)
			t.pool.Put(pkt)
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
			t.pool.Put(pkt)
			continue
		}

		pkt.from = addr
		pkt.len = n

		t.in <- pkt
	}

	close(t.in)

	close(t.done)
}

func (t *UDPTransport) Close() error {
	atomic.StoreInt32(&t.closed, 1)

	if err := t.conn.Close(); err != nil {
		return err
	}

	<-t.done

	return nil
}

func (t *UDPTransport) ReadFrom(msg *proto.GossipMessage) error {
	pkt := <-t.in
	if pkt == nil {
		return ErrClosed
	}

	defer t.pool.Put(pkt)

	if err := protobuf.Unmarshal(pkt.Body(), msg); err != nil {
		return fmt.Errorf("failed to unmarshal gossip message: %w", err)
	}

	return nil
}

func (t *UDPTransport) WriteTo(msg *proto.GossipMessage, peer Addressable) error {
	payload, err := protobuf.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal gossip message: %w", err)
	}

	if len(payload) > maxPayloadSize {
		return ErrMaxSizeExceeded
	}

	_, err = t.conn.WriteToUDP(payload, peer.UDPAddr())
	if err != nil {
		if atomic.LoadInt32(&t.closed) == 1 {
			return ErrClosed
		}

		return fmt.Errorf("failed to send message to upd socket: %w", err)
	}

	return nil
}
