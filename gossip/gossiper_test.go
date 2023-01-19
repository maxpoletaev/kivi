package gossip

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kiwi/gossip/proto"
	"github.com/maxpoletaev/kiwi/gossip/transport"
)

type virtualPeer struct {
	ID       PeerID
	Addr     netip.AddrPort
	Gossiper *Gossiper
	Delegate *MockDelegate
}

type unstableTransport struct {
	Transport
}

func newTransport(bindAddr *netip.AddrPort) *unstableTransport {
	tr, err := transport.Create(bindAddr)
	if err != nil {
		panic("failed to create transport: " + err.Error())
	}

	return &unstableTransport{Transport: tr}
}

func (t *unstableTransport) WriteTo(msg *proto.GossipMessage, addr *netip.AddrPort) error {
	if rand.Intn(10) == 0 {
		return nil
	}

	return t.Transport.WriteTo(msg, addr)
}

func (t *unstableTransport) ReadFrom(msg *proto.GossipMessage) error {
	return t.Transport.ReadFrom(msg)
}

func createTestCluster(n int) ([]*virtualPeer, error) {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	logger = level.NewFilter(logger, level.AllowError())

	nodes := make([]*virtualPeer, 0)
	ok := true

	defer func() {
		if !ok {
			// Shut down all nodes in case of an error.
			for _, node := range nodes {
				node.Gossiper.Shutdown()
			}
		}
	}()

	for id := 1; id <= n; id++ {
		delegate := &MockDelegate{}

		ip := netip.AddrFrom4([4]byte{127, 0, 0, 1})
		addr := netip.AddrPortFrom(ip, uint16(4000+id))

		conf := DefaultConfig()
		conf.Logger = log.WithPrefix(logger, "node_id", id)
		conf.BindAddr = addr.String()
		conf.PeerID = PeerID(id)
		conf.Delegate = delegate

		g, err := Start(conf)
		if err != nil {
			ok = false
			return nil, err
		}

		nodes = append(nodes, &virtualPeer{
			ID:       PeerID(id),
			Addr:     addr,
			Gossiper: g,
			Delegate: delegate,
		})
	}

	for _, node := range nodes {
		for _, other := range nodes {
			if other.ID == node.ID {
				continue
			}

			// Introduce some inconsistency so that nodes do not know directly
			// about each other node. Instead, they should communicate indirectly.
			if rand.Int()%3 == 0 {
				continue
			}

			node.Gossiper.AddPeer(other.ID, other.Addr.String())
		}
	}

	return nodes, nil
}

func TestGossiper_Test(t *testing.T) {
	// Spin up 50 gossip publishers/listeners.
	nodes, err := createTestCluster(50)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	defer func() {
		for _, node := range nodes {
			node.Gossiper.Shutdown()
		}
	}()

	// Generate 100 messages.
	var messages [][]byte
	for i := 0; i < 100; i++ {
		messages = append(messages, []byte(fmt.Sprintf("%d", i)))
	}

	// Node to be used for broadcast. The protocol guarantees that all messages
	// send by one node should be delivered by other nodes in the same order.
	sourceNode := nodes[0]

	// We need the first message to be received first to use it as the starting point.
	err = sourceNode.Gossiper.Broadcast(messages[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait till message reaches all nodes.
	time.Sleep(10 * time.Second)

	// Then broadcast the remaining ones. These may be received by nodes in
	// different order but should still be delivered in the same order.
	for _, payload := range messages[1:] {
		if err = nodes[0].Gossiper.Broadcast(payload); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// Wait till messages reaches all nodes.
	time.Sleep(5 * time.Second)

	// Check the delivered messages on all nodes except the broadcast source.
	for _, node := range nodes {
		if node.ID == sourceNode.ID {
			continue
		}

		ok := true
		delivered := node.Delegate.GetDelivered()

		for i, data := range delivered {
			if !bytes.Equal(messages[i], data) {
				ok = false
				break
			}
		}

		if !ok {
			payloads := make([]string, 0, len(delivered))
			for _, data := range delivered {
				payloads = append(payloads, string(data))
			}

			t.Errorf("node %d: wrong message order: %v", node.ID, payloads)
		}
	}
}
