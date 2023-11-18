package grpc

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/membership/proto"
	"github.com/maxpoletaev/kivi/noderpc"
	replicationpb "github.com/maxpoletaev/kivi/replication/proto"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

var (
	_ noderpc.Client = (*Client)(nil)
)

type Client struct {
	conn              *grpc.ClientConn
	replicationClient replicationpb.ReplicationClient
	storageClient     storagepb.StorageServiceClient
	membershipClient  proto.MembershipClient
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) IsClosed() bool {
	return c.conn.GetState() == connectivity.Shutdown
}

func (c *Client) StorageGet(ctx context.Context, key string) (*noderpc.StorageGetResult, error) {
	resp, err := c.storageClient.Get(ctx, &storagepb.GetRequest{
		Key: key,
	})

	if err != nil {
		return nil, err
	}

	versions := make([]noderpc.VersionedValue, len(resp.Value))

	for idx, v := range resp.Value {
		versions[idx] = noderpc.VersionedValue{
			Tombstone: v.Tombstone,
			Version:   v.Version,
			Data:      v.Data,
		}
	}

	return &noderpc.StorageGetResult{
		Versions: versions,
	}, nil
}

func (c *Client) StoragePut(ctx context.Context, key string, value noderpc.VersionedValue, primary bool) (*noderpc.StoragePutResult, error) {
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

	return &noderpc.StoragePutResult{
		Version: resp.Version,
	}, nil
}

func (c *Client) PullPushState(ctx context.Context, nodes []noderpc.NodeInfo) ([]noderpc.NodeInfo, error) {
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
		case noderpc.NodeStatusHealthy:
			req.Nodes[idx].Status = proto.Status_HEALTHY
		case noderpc.NodeStatusUnhealthy:
			req.Nodes[idx].Status = proto.Status_UNHEALTHY
		case noderpc.NodeStatusLeft:
			req.Nodes[idx].Status = proto.Status_LEFT
		}
	}

	resp, err := c.membershipClient.PullPushState(ctx, req)
	if err != nil {
		return nil, err
	}

	nodes = make([]noderpc.NodeInfo, len(resp.Nodes))
	for idx, n := range resp.Nodes {
		nodes[idx] = noderpc.NodeInfo{
			ID:    noderpc.NodeID(n.Id),
			Name:  n.Name,
			Gen:   n.Generation,
			Addr:  n.Address,
			RunID: n.RunId,
			Error: n.Error,
		}

		switch n.Status {
		case proto.Status_HEALTHY:
			nodes[idx].Status = noderpc.NodeStatusHealthy
		case proto.Status_UNHEALTHY:
			nodes[idx].Status = noderpc.NodeStatusUnhealthy
		case proto.Status_LEFT:
			nodes[idx].Status = noderpc.NodeStatusLeft
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

func (c *Client) PingIndirect(ctx context.Context, nodeID noderpc.NodeID, timeout time.Duration) (noderpc.PingResult, error) {
	resp, err := c.membershipClient.PingIndirect(ctx, &proto.PingIndirectRequest{
		NodeId:  uint32(nodeID),
		Timeout: timeout.Milliseconds(),
	})

	if err != nil {
		return noderpc.PingResult{}, err
	}

	var status noderpc.NodeStatus

	switch resp.Status {
	case proto.Status_HEALTHY:
		status = noderpc.NodeStatusHealthy
	case proto.Status_UNHEALTHY:
		status = noderpc.NodeStatusUnhealthy
	case proto.Status_LEFT:
		status = noderpc.NodeStatusLeft
	}

	return noderpc.PingResult{
		Took:    time.Duration(resp.Duration) * time.Millisecond,
		Message: resp.Message,
		Status:  status,
	}, nil
}

func (c *Client) GetKey(ctx context.Context, key string) (*noderpc.GetKeyResult, error) {
	resp, err := c.replicationClient.Get(ctx, &replicationpb.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, err
	}

	return &noderpc.GetKeyResult{
		Version: resp.Version,
		Values:  resp.Values,
	}, nil
}

func (c *Client) PutKey(ctx context.Context, key string, value []byte, version string) (*noderpc.PutKeyResult, error) {
	resp, err := c.replicationClient.Put(ctx, &replicationpb.PutRequest{
		Key:     key,
		Version: version,
		Value:   value,
	})

	if err != nil {
		if grpcutil.ErrorCode(err) == codes.AlreadyExists {
			return nil, noderpc.ErrVersionConflict
		}

		return nil, err
	}

	return &noderpc.PutKeyResult{
		Acknowledged: int(resp.Acknowledged),
		Version:      resp.Version,
	}, nil
}

func (c *Client) DeleteKey(ctx context.Context, key string, version string) (*noderpc.DeleteKeyResult, error) {
	res, err := c.replicationClient.Delete(ctx, &replicationpb.DeleteRequest{
		Key:     key,
		Version: version,
	})

	if err != nil {
		if grpcutil.ErrorCode(err) == codes.AlreadyExists {
			return nil, noderpc.ErrVersionConflict
		}

		return nil, err
	}

	return &noderpc.DeleteKeyResult{
		Acknowledged: int(res.Acknowledged),
		Version:      res.Version,
	}, nil
}
