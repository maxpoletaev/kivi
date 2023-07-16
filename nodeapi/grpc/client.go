package grpc

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/internal/multierror"
	"github.com/maxpoletaev/kivi/membership/proto"
	"github.com/maxpoletaev/kivi/nodeapi"
	replicationpb "github.com/maxpoletaev/kivi/replication/proto"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

var (
	_ nodeapi.Client = (*Client)(nil)
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

func (c *Client) StorageGet(ctx context.Context, key string) (*nodeapi.StorageGetResult, error) {
	resp, err := c.storageClient.Get(ctx, &storagepb.GetRequest{
		Key: key,
	})

	if err != nil {
		return nil, err
	}

	versions := make([]nodeapi.VersionedValue, len(resp.Value))

	for idx, v := range resp.Value {
		versions[idx] = nodeapi.VersionedValue{
			Tombstone: v.Tombstone,
			Version:   v.Version,
			Data:      v.Data,
		}
	}

	return &nodeapi.StorageGetResult{
		Versions: versions,
	}, nil
}

func (c *Client) StoragePut(ctx context.Context, key string, value nodeapi.VersionedValue, primary bool) (*nodeapi.StoragePutResult, error) {
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

	return &nodeapi.StoragePutResult{
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
			Name:       n.Name,
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
			Name:  n.Name,
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

func (c *Client) Ping(ctx context.Context) (uint64, error) {
	resp, err := c.membershipClient.Ping(ctx, &proto.PingRequest{})
	if err != nil {
		return 0, err
	}

	return resp.StateHash, nil
}

func (c *Client) PingIndirect(ctx context.Context, nodeID nodeapi.NodeID, timeout time.Duration) (nodeapi.PingResult, error) {
	resp, err := c.membershipClient.PingIndirect(ctx, &proto.PingIndirectRequest{
		NodeId:  uint32(nodeID),
		Timeout: timeout.Milliseconds(),
	})

	if err != nil {
		return nodeapi.PingResult{}, err
	}

	var status nodeapi.NodeStatus

	switch resp.Status {
	case proto.Status_HEALTHY:
		status = nodeapi.NodeStatusHealthy
	case proto.Status_UNHEALTHY:
		status = nodeapi.NodeStatusUnhealthy
	case proto.Status_LEFT:
		status = nodeapi.NodeStatusLeft
	}

	return nodeapi.PingResult{
		Took:    time.Duration(resp.Duration) * time.Millisecond,
		Message: resp.Message,
		Status:  status,
	}, nil
}

func (c *Client) GetKey(ctx context.Context, key string) (*nodeapi.GetKeyResult, error) {
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

	return &nodeapi.GetKeyResult{
		Version: resp.Version,
		Values:  values,
	}, nil
}

func (c *Client) PutKey(ctx context.Context, key string, value []byte, version string) (*nodeapi.PutKeyResult, error) {
	resp, err := c.replicationClient.Put(ctx, &replicationpb.PutRequest{
		Key:     key,
		Version: version,
		Value: &replicationpb.Value{
			Data: value,
		},
	})

	if err != nil {
		if grpcutil.ErrorCode(err) == codes.AlreadyExists {
			return nil, nodeapi.ErrVersionConflict
		}

		return nil, err
	}

	return &nodeapi.PutKeyResult{
		Acknowledged: int(resp.Acknowledged),
		Version:      resp.Version,
	}, nil
}

func (c *Client) DeleteKey(ctx context.Context, key string, version string) (*nodeapi.DeleteKeyResult, error) {
	res, err := c.replicationClient.Delete(ctx, &replicationpb.DeleteRequest{
		Key:     key,
		Version: version,
	})

	if err != nil {
		if grpcutil.ErrorCode(err) == codes.AlreadyExists {
			return nil, nodeapi.ErrVersionConflict
		}

		return nil, err
	}

	return &nodeapi.DeleteKeyResult{
		Acknowledged: int(res.Acknowledged),
		Version:      res.Version,
	}, nil
}
