package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	membershippb "github.com/maxpoletaev/kiwi/membership/proto"
	"github.com/maxpoletaev/kiwi/nodeapi"
	storagepb "github.com/maxpoletaev/kiwi/storage/proto"
)

func Dial(ctx context.Context, addr string) (nodeapi.Client, error) {
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

	membershipClient := membershippb.NewMembershipClient(grpcConn)
	storageClient := storagepb.NewStorageServiceClient(grpcConn)

	c := &Client{
		membershipClient: membershipClient,
		storageClient:    storageClient,
	}

	c.addOnCloseHook(func() error {
		return grpcConn.Close()
	})

	return c, nil
}
