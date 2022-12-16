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
	maxPayloadSize    = 1500            // implied by MTU
	receiveBufferSize = 1 * 1024 * 1024 // 1MB
	retryInitialDelay = 30 * time.Millisecond
	retryMaxDelay     = 10 * time.Second
)

var (
	ErrClosed          = errors.New("connection closed")
	ErrMaxSizeExceeded = errors.New("max payload size exceeded")
)

// Addressable is an interface for types that can be converted to UDP address.
type Addressable interface {
	UDPAddr() *net.UDPAddr
}

// UDPTransport is a transport that uses UDP as the underlying protocol.
type UDPTransport struct {
	Logger log.Logger

	bufPool *sync.Pool
	conn    *net.UDPConn
	closed  int32
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
			buf := make([]byte, maxPayloadSize)
			return &buf
		},
	}

	t := &UDPTransport{
		conn:    conn,
		bufPool: pool,
	}

	return t, nil
}

// Close stops the consumer and closes the underlying UDP socket. Once closed,
// the transport cannot be reused. It is safe to call Close multiple times.
// Closing while a read or write is in progress is safe and will cause
// the read or write to return ErrClosed.
func (t *UDPTransport) Close() error {
	if atomic.CompareAndSwapInt32(&t.closed, 0, 1) {
		if err := t.conn.Close(); err != nil {
			return err
		}
	}

	return nil
}

// ReadFrom reads a message from the underlying UDP socket. If the socket is
// closed, ReadFrom returns ErrClosed. It blocks until a message is received
// or the socket is closed.
func (t *UDPTransport) ReadFrom(msg *proto.GossipMessage) error {
	var (
		n    int
		err  error
		addr net.Addr
	)

	retryDelay := retryInitialDelay

	bufPtr := t.bufPool.Get().(*[]byte)
	defer t.bufPool.Put(bufPtr)
	buf := (*bufPtr)[:]

	for {
		n, addr, err = t.conn.ReadFromUDP(buf)

		if err != nil {
			if atomic.LoadInt32(&t.closed) == 1 {
				return ErrClosed
			}

			level.Error(t.Logger).Log("msg", "failed to read from udp", "err", err)

			time.Sleep(retryDelay)

			// Exponential backoff.
			retryDelay *= retryDelay
			if retryDelay > retryMaxDelay {
				retryDelay = retryMaxDelay
			}

			continue
		}

		if n == 0 {
			level.Warn(t.Logger).Log("msg", "received empty udp packet", "from", addr)
			continue
		}

		retryDelay = retryInitialDelay

		break
	}

	if err := protobuf.Unmarshal(buf[:n], msg); err != nil {
		return fmt.Errorf("failed to unmarshal gossip message: %w", err)
	}

	return nil
}

// WriteTo writes a message to the given address. If the socket is closed,
// WriteTo returns ErrClosed. It blocks until the message is sent or the
// socket is closed.
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
