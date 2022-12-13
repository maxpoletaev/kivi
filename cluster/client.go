package cluster

//go:generate mockgen -source=client.go -destination=mock/client_mock.go -package=mock

import (
	"context"

	faildetectorpb "github.com/maxpoletaev/kv/faildetector/proto"
	membershippb "github.com/maxpoletaev/kv/membership/proto"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
)

// Client is an interface to interact with a remote cluster member.
type Client interface {
	Join(ctx context.Context, req *membershippb.JoinRequest) (*membershippb.JoinResponse, error)
	Members(ctx context.Context) (*membershippb.MembersResponse, error)
	Get(ctx context.Context, req *storagepb.GetRequest) (*storagepb.GetResponse, error)
	Put(ctx context.Context, req *storagepb.PutRequest) (*storagepb.PutResponse, error)
	PingDirect(ctx context.Context) (*faildetectorpb.PingResponse, error)
	PingIndirect(ctx context.Context, req *faildetectorpb.PingRequest) (*faildetectorpb.PingResponse, error)
	IsClosed() bool
	Close() error
}
