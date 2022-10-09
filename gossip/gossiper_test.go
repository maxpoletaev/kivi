package gossip

import (
	"bytes"
	"math/rand"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
)

type delegateMock struct {
	mut       sync.Mutex
	received  [][]byte
	delivered [][]byte
}

func (d *delegateMock) Receive(payload []byte) error {
	d.mut.Lock()
	d.received = append(d.received, payload)
	d.mut.Unlock()

	return nil
}

func (d *delegateMock) GetReceived() [][]byte {
	d.mut.Lock()
	received := d.received
	d.mut.Unlock()
	return received
}

func (d *delegateMock) Deliver(payload []byte) error {
	d.mut.Lock()
	d.delivered = append(d.delivered, payload)
	d.mut.Unlock()

	return nil
}

func (d *delegateMock) GetDelivered() [][]byte {
	d.mut.Lock()
	delivered := d.delivered
	d.mut.Unlock()
	return delivered
}

type virtualPeer struct {
	ID       PeerID
	Addr     netip.AddrPort
	Gossiper *Gossiper
	Delegate *delegateMock
}

func createTestCluster(n int) ([]*virtualPeer, error) {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
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
		delegate := &delegateMock{}

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

			node.Gossiper.Register(other.ID, other.Addr.String())
		}
	}

	return nodes, nil
}

// The execution of this test is non-deterministic, and it may sometimes fail
// with a couple of nodes not delivering messages correctly. This is fine as
// the algorithm is probabilistic and does not guarantee a 100% delivery. As
// long as the error rate is within 1-2%, we are OK. Later I’ll find a better
// way of handling this.
func TestGossiper(t *testing.T) {
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

	messages := [][]byte{
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
		[]byte("5"),
		[]byte("6"),
		[]byte("7"),
		[]byte("8"),
		[]byte("9"),
		[]byte("10"),
	}

	// Node to be used for broadcast. The protocol guarantess that all messages
	// send by one node should be delivered by other nodes in the same order.
	sourceNode := nodes[0]

	// We need the first message to to be received first to use it as the starting point.
	err = sourceNode.Gossiper.Broadcast(messages[0])
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait till message reaches all nodes.
	time.Sleep(1 * time.Second)

	// Then broadcast the remaining ones. These may be received by nodes in
	// different order but should still be delivered in the same order.
	for _, payload := range messages[1:] {
		err = nodes[0].Gossiper.Broadcast(payload)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// Wait till messages reaches all nodes.
	time.Sleep(1 * time.Second)

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
