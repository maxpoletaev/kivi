package nodeclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/maxpoletaev/kv/cluster"
	faildetectorpb "github.com/maxpoletaev/kv/faildetector/proto"
	membershippb "github.com/maxpoletaev/kv/membership/proto"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
)

// GRPCDialer is a factory for creating GRPC connections to cluster members.
type GRPCDialer struct{}

// NewDialer creates a new GRPCDialer.
func NewDialer() *GRPCDialer {
	return &GRPCDialer{}
}

// DialContext creates a new GRPC connection to the given address. It will block until the
// connection is established and ready or the context is canceled.
func (d *GRPCDialer) DialContext(ctx context.Context, addr string) (cluster.Client, error) {
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

	faildetectorClient := faildetectorpb.NewFailDetectorServiceClient(grpcConn)
	membershipClient := membershippb.NewMembershipServiceClient(grpcConn)
	storageClient := storagepb.NewStorageServiceClient(grpcConn)

	c := &Conn{
		faildetectorClient: faildetectorClient,
		membershipClient:   membershipClient,
		storageClient:      storageClient,
	}

	c.addOnCloseHook(func() error {
		return grpcConn.Close()
	})

	return c, nil
}
