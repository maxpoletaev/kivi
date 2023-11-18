package nodeapi

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"

	membershippb "github.com/maxpoletaev/kivi/membership/proto"
	replicationpb "github.com/maxpoletaev/kivi/replication/proto"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

// Dialer is a function that establishes a connection with a cluster node.
type Dialer func(ctx context.Context, addr string) (*Client, error)

// DialGRPC establishes a gRPC connection with a cluster node.
func DialGRPC(ctx context.Context, addr string) (*Client, error) {
	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 10 * time.Second, // ping every 10 seconds if there is no activity
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
	)

	if err != nil {
		return nil, fmt.Errorf("grpc dial failed: %w", err)
	}

	replicationClient := replicationpb.NewReplicationClient(conn)
	membershipClient := membershippb.NewMembershipClient(conn)
	storageClient := storagepb.NewStorageClient(conn)

	return &Client{
		conn:        conn,
		Storage:     storageClient,
		Membership:  membershipClient,
		Replication: replicationClient,
	}, nil
}
