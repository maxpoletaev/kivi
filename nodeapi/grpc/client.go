package grpc

import (
	"context"
	"sync/atomic"

	"github.com/maxpoletaev/kivi/internal/multierror"
	"github.com/maxpoletaev/kivi/membership/proto"
	"github.com/maxpoletaev/kivi/nodeapi"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

var (
	_ nodeapi.Client = (*Client)(nil)
)

type Client struct {
	storageClient    storagepb.StorageServiceClient
	membershipClient proto.MembershipClient
	onClose          []func() error
	closed           uint32
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

func (c *Client) Get(ctx context.Context, key string) ([]nodeapi.VersionedValue, error) {
	resp, err := c.storageClient.Get(ctx, &storagepb.GetRequest{
		Key: key,
	})

	if err != nil {
		return nil, err
	}

	values := make([]nodeapi.VersionedValue, len(resp.Value))
	for idx, v := range resp.Value {
		values[idx] = nodeapi.VersionedValue{
			Tombstone: v.Tombstone,
			Version:   v.Version,
			Data:      v.Data,
		}
	}

	return values, nil
}

func (c *Client) Put(ctx context.Context, key string, value nodeapi.VersionedValue, primary bool) (*nodeapi.PutResponse, error) {
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

	return &nodeapi.PutResponse{
		Version: resp.Version,
	}, nil
}

func (c *Client) PullPushState(ctx context.Context, nodes []nodeapi.NodeInfo) ([]nodeapi.NodeInfo, error) {
	req := &proto.PullPushStateRequest{
		Nodes: make([]*proto.Node, len(nodes)),
	}

	for idx, n := range nodes {
		req.Nodes[idx] = &proto.Node{
			Id:         uint32(n.ID),
			Address:    n.Addr,
			Generation: n.Gen,
			Error:      n.Error,
			RunId:      n.RunID,
		}

		switch n.Status {
		case nodeapi.NodeStatusHealthy:
			req.Nodes[idx].Status = proto.Status_HEALTHY
		case nodeapi.NodeStatusUnhealthy:
			req.Nodes[idx].Status = proto.Status_UNHEALTHY
		case nodeapi.NodeStatusLeft:
			req.Nodes[idx].Status = proto.Status_LEFT
		}
	}

	resp, err := c.membershipClient.PullPushState(ctx, req)
	if err != nil {
		return nil, err
	}

	nodes = make([]nodeapi.NodeInfo, len(resp.Nodes))
	for idx, n := range resp.Nodes {
		nodes[idx] = nodeapi.NodeInfo{
			ID:    nodeapi.NodeID(n.Id),
			Gen:   n.Generation,
			Addr:  n.Address,
			RunID: n.RunId,
			Error: n.Error,
		}

		switch n.Status {
		case proto.Status_HEALTHY:
			nodes[idx].Status = nodeapi.NodeStatusHealthy
		case proto.Status_UNHEALTHY:
			nodes[idx].Status = nodeapi.NodeStatusUnhealthy
		case proto.Status_LEFT:
			nodes[idx].Status = nodeapi.NodeStatusLeft
		}
	}

	return nodes, nil
}
