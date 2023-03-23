package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	membershippb "github.com/maxpoletaev/kivi/membership/proto"
	"github.com/maxpoletaev/kivi/nodeclient"
	replicationpb "github.com/maxpoletaev/kivi/replication/proto"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

func Dial(ctx context.Context, addr string) (nodeclient.Conn, error) {
	creds := insecure.NewCredentials()

	grpcConn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial failed: %w", err)
	}

	replicationClient := replicationpb.NewReplicationClient(grpcConn)
	membershipClient := membershippb.NewMembershipClient(grpcConn)
	storageClient := storagepb.NewStorageServiceClient(grpcConn)

	c := &Client{
		replicationClient: replicationClient,
		membershipClient:  membershipClient,
		storageClient:     storageClient,
	}

	c.addOnCloseHook(func() error {
		return grpcConn.Close()
	})

	return c, nil
}
