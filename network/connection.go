package network

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	networkpb "github.com/maxpoletaev/kv/network/proto"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
)

type Connection struct {
	conn    *grpc.ClientConn
	Cluster networkpb.ClusterSericeClient
	Storage storagepb.StorageServiceClient
}

func (c *Connection) IsReady() bool {
	return c.conn.GetState() == connectivity.Ready
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func defaultDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func connect(ctx context.Context, addr string) (*Connection, error) {
	opts := defaultDialOptions()

	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial failed: %w", err)
	}

	storageClient := storagepb.NewStorageServiceClient(conn)
	clusterClient := networkpb.NewClusterSericeClient(conn)

	return &Connection{
		conn:    conn,
		Storage: storageClient,
		Cluster: clusterClient,
	}, nil
}
