package membership_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeapi"
	nodeapimock "github.com/maxpoletaev/kivi/nodeapi/mock"
)

func TestCluster_Conn(t *testing.T) {
	ctrl := gomock.NewController(t)

	client := &nodeapi.Client{
		Storage:    nodeapimock.NewMockStorageServiceClient(ctrl),
		Membership: nodeapimock.NewMockMembershipClient(ctrl),
	}

	var dialerCalled int

	conf := membership.DefaultConfig()
	conf.NodeID = 1

	conf.Dialer = func(ctx context.Context, addr string) (*nodeapi.Client, error) {
		dialerCalled++
		return client, nil
	}

	cluster := membership.NewCluster(conf)
	defer cluster.Leave(context.Background())

	client2, err := cluster.Conn(cluster.SelfID())
	require.NoError(t, err)
	require.Equal(t, client, client2)
	require.Equal(t, 1, dialerCalled)
}

func TestCluster_Conn_AlreadyConnected(t *testing.T) {
	conf := membership.DefaultConfig()
	conf.Dialer = func(ctx context.Context, addr string) (*nodeapi.Client, error) {
		t.Fatal("dialer should not be called")
		return nil, nil
	}

	cluster := membership.NewCluster(conf)
	defer cluster.Leave(context.Background())

	conn := &nodeapi.Client{}
	cluster.AddConn(cluster.SelfID(), conn)

	conn2, err := cluster.Conn(cluster.SelfID())
	require.NoError(t, err)
	require.Equal(t, conn, conn2)
}

func TestCluster_Conn_Concurrent(t *testing.T) {
	var dialerCalled int32

	clusterConf := membership.DefaultConfig()
	clusterConf.NodeID = 1

	clusterConf.Dialer = func(ctx context.Context, addr string) (*nodeapi.Client, error) {
		atomic.AddInt32(&dialerCalled, 1)
		time.Sleep(100 * time.Millisecond) // Simulate membership latency.
		conn := &nodeapi.Client{}
		return conn, nil
	}

	concurrency := 10
	errs := make([]error, concurrency)
	connections := make([]*nodeapi.Client, concurrency)

	cluster := membership.NewCluster(clusterConf)
	defer cluster.Leave(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	begin := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			<-begin

			conn, err := cluster.Conn(1)
			connections[i] = conn
			errs[i] = err
		}(i)
	}

	close(begin)
	wg.Wait()

	for i := 0; i < concurrency; i++ {
		if err := errs[i]; err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}

		if connections[i] == nil {
			t.Fatalf("connection %d: is nil", i)
		}
	}

	require.Equal(t, int32(1), dialerCalled)
}
