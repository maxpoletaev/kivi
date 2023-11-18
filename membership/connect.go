package membership

import (
	"context"
	"fmt"

	"github.com/go-kit/log/level"

	"github.com/maxpoletaev/kivi/nodeapi"
)

func (cl *SWIMCluster) loadConn(id NodeID) (*nodeapi.Client, bool) {
	cl.mut.RLock()

	conn, ok := cl.connections[id]
	if !ok {
		cl.mut.RUnlock()
		return nil, false
	}

	// The connection is present but was closed manually, so it is not usable.
	// Need to re-acquire the lock and remove it from the registry.
	if conn.IsClosed() {
		cl.mut.RUnlock()
		cl.mut.Lock()

		// A new connection might have been created while we were waiting for the lock.
		if conn, ok := cl.connections[id]; ok && !conn.IsClosed() {
			cl.mut.Unlock()
			return conn, true
		}

		// Still closed? Remove it from the registry.
		delete(cl.connections, id)

		cl.mut.Unlock()

		return nil, false
	}

	cl.mut.RUnlock()

	return conn, ok
}

func (cl *SWIMCluster) connect(ctx context.Context, id NodeID) (*nodeapi.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, cl.dialTimeout)
	defer cancel()

	var (
		retry  bool
		loaded bool
		done   chan struct{}
	)

	for {
		d := make(chan struct{})

		// Check if there is already a goroutine dialing the node.
		// If so, we will wait for it to finish or for the context to expire.
		done, loaded = cl.waiting.LoadOrStore(id, d)
		if !loaded {
			break
		}

		// Since LoadOrStore failed, there is already a channel in the map,
		// so we need to discard the one we just created.
		close(d)

		select {
		case <-done:
			// noop
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		var (
			conn *nodeapi.Client
			ok   bool
		)

		// Try to load the connection created in another goroutine.
		if withLock(cl.mut.RLocker(), func() {
			conn, ok = cl.connections[id]
		}); ok {
			return conn, nil
		}

		// The other goroutine has failed to connect to the node. Make one more attempt.
		if !retry {
			retry = true
			continue
		}

		// We have already retried with no luck.
		return nil, fmt.Errorf("failed to connect in another goroutine")
	}

	// We are the one dialing the node.
	defer cl.waiting.Delete(id)
	defer close(done)

	var (
		node Node
		ok   bool
	)

	if withLock(cl.mut.RLocker(), func() {
		node, ok = cl.nodes[id]
	}); !ok {
		return nil, fmt.Errorf("node not found")
	}

	dialAddr := node.PublicAddr
	if node.LocalAddr != "" {
		dialAddr = node.LocalAddr
	}

	// Dial the node, this may take a while.
	conn, err := cl.dialer(ctx, dialAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", dialAddr, err)
	}

	// Check if the connection has been added while we were dialing.
	// If so, discard the connection we just created and use the existing one.
	withLock(&cl.mut, func() {
		actual, ok := cl.connections[id]

		if ok && !actual.IsClosed() {
			if err := conn.Close(); err != nil {
				level.Warn(cl.logger).Log("msg", "failed to close connection", "node", id, "err", err)
			}

			conn = actual

			return
		}

		cl.connections[id] = conn
	})

	return conn, nil
}

// Conn returns a connection to the member with the given ID. If the connection
// is not present, it attempts to dial the member and create a new connection.
func (cl *SWIMCluster) Conn(id NodeID) (*nodeapi.Client, error) {
	if conn, ok := cl.loadConn(id); ok {
		return conn, nil
	}

	return cl.connect(context.Background(), id)
}

// ConnContext returns a connection to the member with the given ID. If the
// connection is not present, it attempts to dial the member and create a new
// connection. The context is used to cancel the dialing process.
func (cl *SWIMCluster) ConnContext(ctx context.Context, id NodeID) (*nodeapi.Client, error) {
	if conn, ok := cl.loadConn(id); ok {
		return conn, nil
	}

	return cl.connect(ctx, id)
}

// LocalConn returns a connection to the local member. It assumes that the local
// connection is always stable, so it panics if it is not present.
func (cl *SWIMCluster) LocalConn() *nodeapi.Client {
	var (
		conn *nodeapi.Client
		err  error
		ok   bool
	)

	if conn, ok = cl.loadConn(cl.selfID); ok {
		return conn
	}

	if conn, err = cl.connect(context.Background(), cl.selfID); err != nil {
		panic(fmt.Sprintf("not connected to self: %v", err))
	}

	return conn
}

// AddConn adds a connection to the cluster. If a connection to the same member
// already exists, the old connection is closed. This method is intended to be
// used during tests or during the cluster bootstrap.
func (cl *SWIMCluster) AddConn(id NodeID, conn *nodeapi.Client) {
	cl.mut.Lock()
	defer cl.mut.Unlock()

	if actual, ok := cl.connections[id]; ok {
		if err := actual.Close(); err != nil {
			level.Warn(cl.logger).Log("msg", "failed to close connection", "node", id, "err", err)
		}
	}

	cl.connections[id] = conn
}
