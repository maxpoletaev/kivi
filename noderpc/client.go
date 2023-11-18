package noderpc

//go:generate mockgen -destination=mock/client_mock.go -package=mock github.com/maxpoletaev/kivi/nodeapi Conn

import "context"

// Client is a client to a cluster node.
type Client interface {
	storageClient
	membershipClient
	replicationClient

	IsClosed() bool
	Close() error
}

// Dialer is a function that establishes a connection with a cluster node.
type Dialer func(ctx context.Context, addr string) (Client, error)
