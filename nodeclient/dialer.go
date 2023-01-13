package nodeclient

//go:generate mockgen -source=dialer.go -destination=dialer_mock.go -package=clust

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	faildetectorpb "github.com/maxpoletaev/kiwi/faildetector/proto"
	membershippb "github.com/maxpoletaev/kiwi/membership/proto"
	storagepb "github.com/maxpoletaev/kiwi/storage/proto"
)

// Dialer is used to create new connections to the cluster members.
type Dialer interface {
	DialContext(ctx context.Context, addr string) (Conn, error)
}

// GrpcDialer is a factory for creating GRPC connections to cluster members.
type GrpcDialer struct{}

// NewGrpcDialer creates a new GRPCDialer.
func NewGrpcDialer() *GrpcDialer {
	return &GrpcDialer{}
}

// DialContext creates a new GRPC connection to the given address. It will block until the
// connection is established and ready or the context is canceled.
func (d *GrpcDialer) DialContext(ctx context.Context, addr string) (Conn, error) {
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

	c := &GrpcConn{
		faildetectorClient: faildetectorClient,
		membershipClient:   membershipClient,
		storageClient:      storageClient,
	}

	c.addOnCloseHook(func() error {
		return grpcConn.Close()
	})

	return c, nil
}
