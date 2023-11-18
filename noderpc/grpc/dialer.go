package grpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"

	membershippb "github.com/maxpoletaev/kivi/membership/proto"
	"github.com/maxpoletaev/kivi/noderpc"
	replicationpb "github.com/maxpoletaev/kivi/replication/proto"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

func Dial(ctx context.Context, addr string) (noderpc.Client, error) {
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
	storageClient := storagepb.NewStorageServiceClient(conn)

	c := &Client{
		replicationClient: replicationClient,
		membershipClient:  membershipClient,
		storageClient:     storageClient,
		conn:              conn,
	}

	return c, nil
}
