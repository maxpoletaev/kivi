package nodeapi

//go:generate mockgen -destination=mock/storage_client_gen.go -package=mock github.com/maxpoletaev/kivi/storage/proto StorageServiceClient
//go:generate mockgen -destination=mock/membership_client_gen.go -package=mock github.com/maxpoletaev/kivi/membership/proto MembershipClient
//go:generate mockgen -destination=mock/replication_client_gen.go -package=mock github.com/maxpoletaev/kivi/replication/proto ReplicationClient

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	membershippb "github.com/maxpoletaev/kivi/membership/proto"
	replicationpb "github.com/maxpoletaev/kivi/replication/proto"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

// Client is a wrapper around gRPC clients for different services.
type Client struct {
	conn        *grpc.ClientConn
	Storage     storagepb.StorageClient
	Membership  membershippb.MembershipClient
	Replication replicationpb.ReplicationClient
}

// IsClosed returns true if the connection is closed.
func (c *Client) IsClosed() bool {
	if c.conn == nil {
		return false // mocked
	}

	return c.conn.GetState() == connectivity.Shutdown
}

// Close closes the connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}

	return c.conn.Close()
}
