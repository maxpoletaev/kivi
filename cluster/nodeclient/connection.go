package nodeclient

import (
	"context"
	"sync/atomic"

	"google.golang.org/protobuf/types/known/emptypb"

	faildetectorpb "github.com/maxpoletaev/kv/faildetector/proto"
	"github.com/maxpoletaev/kv/internal/multierror"
	membershippb "github.com/maxpoletaev/kv/membership/proto"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
)

// Conn encapsulates several GRPC clients to interact with cluster members.
type Conn struct {
	faildetectorClient faildetectorpb.FailDetectorServiceClient
	membershipClient   membershippb.MembershipServiceClient
	storageClient      storagepb.StorageServiceClient
	onClose            []func() error
	closed             uint32
}

func (c *Conn) addOnCloseHook(f func() error) {
	c.onClose = append(c.onClose, f)
}

// Close closes the underlying GRPC connection. Please note that the connection may
// be used by other goroutines and closing it may cause some operations to fail.
func (c *Conn) Close() error {
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return nil // already closed
	}

	errs := multierror.New[int]()
	for idx, f := range c.onClose {
		if err := f(); err != nil {
			errs.Add(idx, err)
		}
	}

	return errs.Ret()
}

// IsClosed returns true if the connection is closed.
func (c *Conn) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

// Get returns the value for the given key. If the key does not exist, it returns an empty array.
func (c *Conn) Get(ctx context.Context, req *storagepb.GetRequest) (*storagepb.GetResponse, error) {
	return c.storageClient.Get(ctx, req)
}

// Put sets the value for the given key. If the key already exists, it is overwritten.
func (c *Conn) Put(ctx context.Context, req *storagepb.PutRequest) (*storagepb.PutResponse, error) {
	return c.storageClient.Put(ctx, req)
}

// Join attempts to join the cluster. It returns the list of current cluster members before the join.
func (c *Conn) Join(ctx context.Context, req *membershippb.JoinRequest) (*membershippb.JoinResponse, error) {
	return c.membershipClient.Join(ctx, req)
}

// Info returns the info about all cluster members.
func (c *Conn) Members(ctx context.Context) (*membershippb.MembersResponse, error) {
	return c.membershipClient.Members(ctx, &emptypb.Empty{})
}

// PingDirect sends a ping to the given node directly.
func (c *Conn) PingDirect(ctx context.Context) (*faildetectorpb.PingResponse, error) {
	return c.faildetectorClient.Ping(ctx, &faildetectorpb.PingRequest{})
}

// PingIndirect sends a ping to the given node through an intermediate node.
func (c *Conn) PingIndirect(ctx context.Context, req *faildetectorpb.PingRequest) (*faildetectorpb.PingResponse, error) {
	return c.faildetectorClient.Ping(ctx, req)
}
