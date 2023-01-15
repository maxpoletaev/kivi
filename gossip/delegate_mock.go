package gossip

import "sync"

type MockDelegate struct {
	mut       sync.Mutex
	received  [][]byte
	delivered [][]byte
}

func (d *MockDelegate) Receive(payload []byte) error {
	d.mut.Lock()
	d.received = append(d.received, payload)
	d.mut.Unlock()

	return nil
}

func (d *MockDelegate) GetReceived() [][]byte {
	d.mut.Lock()
	received := d.received
	d.mut.Unlock()

	return received
}

func (d *MockDelegate) Deliver(payload []byte) error {
	d.mut.Lock()
	d.delivered = append(d.delivered, payload)
	d.mut.Unlock()

	return nil
}

func (d *MockDelegate) GetDelivered() [][]byte {
	d.mut.Lock()
	delivered := d.delivered
	d.mut.Unlock()

	return delivered
}
