package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"

	membershippb "github.com/maxpoletaev/kivi/membership/proto"
	"github.com/maxpoletaev/kivi/nodeapi"
	replicationpb "github.com/maxpoletaev/kivi/replication/proto"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

func Dial(ctx context.Context, addr string) (nodeapi.Client, error) {
	creds := insecure.NewCredentials()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
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
	}

	c.addOnCloseHook(conn.Close)

	return c, nil
}
