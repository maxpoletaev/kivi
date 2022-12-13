package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/maxpoletaev/kv/internal/generic"
	"github.com/maxpoletaev/kv/membership"
)

type ConnRegistry struct {
	mut            sync.RWMutex
	connections    map[membership.NodeID]Client
	inProgress     generic.SyncMap[membership.NodeID, chan struct{}]
	members        MemberRegistry
	connectTimeout time.Duration
	dialer         Dialer
}

func NewConnRegistry(members MemberRegistry, dialer Dialer) *ConnRegistry {
	return &ConnRegistry{
		connections:    make(map[membership.NodeID]Client),
		connectTimeout: 5 * time.Second,
		members:        members,
		dialer:         dialer,
	}
}

func (r *ConnRegistry) get(id membership.NodeID) (Client, bool) {
	r.mut.RLock()

	conn, ok := r.connections[id]
	if !ok {
		r.mut.RUnlock()
		return nil, false
	}

	// The connection is present but was closed manually, so it is not usable.
	// Need to reaqcquire the lock and remove it from the registry.
	if conn.IsClosed() {
		r.mut.RUnlock()
		r.mut.Lock()

		// A new connection might have been created while we were waiting for the lock.
		if conn, ok := r.connections[id]; ok && !conn.IsClosed() {
			r.mut.Unlock()
			return conn, true
		}

		// Still closed? Remove it from the registry.
		delete(r.connections, id)
		r.mut.Unlock()

		return nil, false
	}

	r.mut.RUnlock()

	return conn, ok
}

func (r *ConnRegistry) connect(id membership.NodeID) (Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.connectTimeout)
	defer cancel()

	var retry bool

	for {
		c := make(chan struct{})

		done, loaded := r.inProgress.LoadOrStore(id, c)

		// Store failed means another goroutine is already dialing the member.
		// Wait for it to finish or for the context to expire.
		if loaded {
			close(c)

			select {
			case <-done:
				// noop
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			r.mut.RLock()

			// Try to get the connection created by the other goroutine.
			if conn, ok := r.connections[id]; ok {
				r.mut.RUnlock()
				return conn, nil
			}

			r.mut.RUnlock()

			// The other goroutine has failed to connect to the member. Make one more attempt.
			if !retry {
				retry = true
				continue
			}

			// We have already retried with no luck.
			return nil, fmt.Errorf("failed to connect in another goroutine")
		}

		defer r.inProgress.Delete(id)
		defer close(done)

		// Now we are the one dialing the member.
		member, ok := r.members.Member(id)
		if !ok {
			return nil, fmt.Errorf("member not found")
		}

		conn, err := r.dialer.DialContext(ctx, member.ServerAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to dial %s: %w", member.ServerAddr, err)
		}

		r.mut.Lock()
		defer r.mut.Unlock()

		// Check if the connection has been added manually while we were dialing.
		// If so, discard the connection we just created and use the existing one.
		if old, ok := r.connections[id]; ok && !old.IsClosed() {
			_ = conn.Close()
			return old, nil
		}

		r.connections[id] = conn

		return conn, nil
	}
}

// CollectGarbage closes and removes the connections that are closed or
// belong to members that are no longer in the cluster.
func (r *ConnRegistry) CollectGarbage() {
	r.mut.Lock()
	defer r.mut.Unlock()

	for id, conn := range r.connections {
		if !r.members.HasMember(id) {
			_ = conn.Close()
		}

		// Remove all closed connections. They may have been closed manually.
		if conn.IsClosed() {
			delete(r.connections, id)
		}
	}
}

// Get returns a connection to the member with the given ID. If the connection
// is not present, it attempts to dial the member and create a new connection.
func (r *ConnRegistry) Get(id membership.NodeID) (Client, error) {
	if conn, ok := r.get(id); ok {
		return conn, nil
	}

	return r.connect(id)
}

// Add adds a connection to the registry. If a connection to the member with
// the same ID already exists, the old connection is closed.
func (r *ConnRegistry) Put(id membership.NodeID, conn Client) {
	r.mut.Lock()
	defer r.mut.Unlock()

	if old, ok := r.connections[id]; ok {
		_ = old.Close()
	}

	r.connections[id] = conn
}
