package nodeclient

//go:generate mockgen -destination=mock/client_mock.go -package=mock github.com/maxpoletaev/kivi/nodeclient Conn

import "context"

// Conn is a client to a cluster node.
type Conn interface {
	storageClient
	clusterClient
	replicationClient

	// IsClosed returns true if the connection to the cluster node is closed, and
	// connection to the node cannot be used. This method is not intended to be
	// called during normal operation, but is rather used by the cluster connection
	// manager.
	IsClosed() bool

	// Close closes the connection to the cluster node. This method should be called
	// once the client is no longer needed, such as when the node is removed from the
	// cluster. It is not safe to call this method on a healthy node as the
	// connection may be in use by other goroutines.
	Close() error
}

// Dialer is a function that establishes a connection with a cluster node.
type Dialer func(ctx context.Context, addr string) (Conn, error)
