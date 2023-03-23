package grpc

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/internal/multierror"
	"github.com/maxpoletaev/kivi/membership/proto"
	"github.com/maxpoletaev/kivi/nodeclient"
	replicationpb "github.com/maxpoletaev/kivi/replication/proto"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

var (
	_ nodeclient.Conn = (*Client)(nil)
)

type Client struct {
	replicationClient replicationpb.ReplicationClient
	storageClient     storagepb.StorageServiceClient
	membershipClient  proto.MembershipClient
	onClose           []func() error
	closed            uint32
}

func (c *Client) addOnCloseHook(f func() error) {
	c.onClose = append(c.onClose, f)
}

func (c *Client) Close() error {
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return nil // already closed
	}

	errs := multierror.New[int]()

	for idx, f := range c.onClose {
		if err := f(); err != nil {
			errs.Add(idx, err)
		}
	}

	return errs.Combined()
}

func (c *Client) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

func (c *Client) StorageGet(ctx context.Context, key string) ([]nodeclient.VersionedValue, error) {
	resp, err := c.storageClient.Get(ctx, &storagepb.GetRequest{
		Key: key,
	})

	if err != nil {
		return nil, err
	}

	values := make([]nodeclient.VersionedValue, len(resp.Value))
	for idx, v := range resp.Value {
		values[idx] = nodeclient.VersionedValue{
			Tombstone: v.Tombstone,
			Version:   v.Version,
			Data:      v.Data,
		}
	}

	return values, nil
}

func (c *Client) StoragePut(ctx context.Context, key string, value nodeclient.VersionedValue, primary bool) (*nodeclient.PutResponse, error) {
	resp, err := c.storageClient.Put(ctx, &storagepb.PutRequest{
		Key:     key,
		Primary: primary,
		Value: &storagepb.VersionedValue{
			Data:      value.Data,
			Version:   value.Version,
			Tombstone: value.Tombstone,
		},
	})

	if err != nil {
		return nil, err
	}

	return &nodeclient.PutResponse{
		Version: resp.Version,
	}, nil
}

func (c *Client) PullPushState(ctx context.Context, nodes []nodeclient.NodeInfo) ([]nodeclient.NodeInfo, error) {
	req := &proto.PullPushStateRequest{
		Nodes: make([]*proto.Node, len(nodes)),
	}

	for idx, n := range nodes {
		req.Nodes[idx] = &proto.Node{
			Id:         uint32(n.ID),
			Name:       n.Name,
			Address:    n.Addr,
			Generation: n.Gen,
			Error:      n.Error,
			RunId:      n.RunID,
		}

		switch n.Status {
		case nodeclient.NodeStatusHealthy:
			req.Nodes[idx].Status = proto.Status_HEALTHY
		case nodeclient.NodeStatusUnhealthy:
			req.Nodes[idx].Status = proto.Status_UNHEALTHY
		case nodeclient.NodeStatusLeft:
			req.Nodes[idx].Status = proto.Status_LEFT
		}
	}

	resp, err := c.membershipClient.PullPushState(ctx, req)
	if err != nil {
		return nil, err
	}

	nodes = make([]nodeclient.NodeInfo, len(resp.Nodes))
	for idx, n := range resp.Nodes {
		nodes[idx] = nodeclient.NodeInfo{
			ID:    nodeclient.NodeID(n.Id),
			Name:  n.Name,
			Gen:   n.Generation,
			Addr:  n.Address,
			RunID: n.RunId,
			Error: n.Error,
		}

		switch n.Status {
		case proto.Status_HEALTHY:
			nodes[idx].Status = nodeclient.NodeStatusHealthy
		case proto.Status_UNHEALTHY:
			nodes[idx].Status = nodeclient.NodeStatusUnhealthy
		case proto.Status_LEFT:
			nodes[idx].Status = nodeclient.NodeStatusLeft
		}
	}

	return nodes, nil
}

func (c *Client) RepGet(ctx context.Context, key string) (*nodeclient.RepGetResponse, error) {
	resp, err := c.replicationClient.Get(ctx, &replicationpb.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, err
	}

	values := make([][]byte, len(resp.Values))
	for idx, v := range resp.Values {
		values[idx] = v.Data
	}

	return &nodeclient.RepGetResponse{
		Version: resp.Version,
		Values:  values,
	}, nil
}

func (c *Client) RepPut(ctx context.Context, key string, value []byte, version string) (string, error) {
	resp, err := c.replicationClient.Put(ctx, &replicationpb.PutRequest{
		Key:     key,
		Version: version,
		Value: &replicationpb.Value{
			Data: value,
		},
	})

	if err != nil {
		if grpcutil.ErrorCode(err) == codes.AlreadyExists {
			return "", nodeclient.ErrVersionConflict
		}

		return "", err
	}

	return resp.Version, nil
}

func (c *Client) RepDelete(ctx context.Context, key string, version string) (string, error) {
	res, err := c.replicationClient.Delete(ctx, &replicationpb.DeleteRequest{
		Key:     key,
		Version: version,
	})

	if err != nil {
		if grpcutil.ErrorCode(err) == codes.AlreadyExists {
			return "", nodeclient.ErrVersionConflict
		}

		return "", err
	}

	return res.Version, nil
}
