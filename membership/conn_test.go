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
	nodeclientmock "github.com/maxpoletaev/kivi/nodeapi/mock"
)

func TestCluster_Conn(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := nodeclientmock.NewMockConn(ctrl)
	conn.EXPECT().Close().Return(nil).Times(1)

	var dialerCalled int

	conf := membership.DefaultConfig()
	conf.NodeID = 1

	conf.Dialer = func(ctx context.Context, addr string) (nodeapi.Client, error) {
		dialerCalled++
		return conn, nil
	}

	cluster := membership.NewSWIM(conf)
	defer cluster.Leave(context.Background())

	conn2, err := cluster.Conn(cluster.SelfID())
	require.NoError(t, err)
	require.Equal(t, conn, conn2)
	require.Equal(t, 1, dialerCalled)
}

func TestCluster_Conn_AlreadyConnected(t *testing.T) {
	conf := membership.DefaultConfig()
	conf.Dialer = func(ctx context.Context, addr string) (nodeapi.Client, error) {
		t.Fatal("dialer should not be called")
		return nil, nil
	}

	cluster := membership.NewSWIM(conf)
	defer cluster.Leave(context.Background())

	ctrl := gomock.NewController(t)
	conn := nodeclientmock.NewMockConn(ctrl)
	conn.EXPECT().IsClosed().Return(false).Times(1)
	conn.EXPECT().Close().Return(nil).Times(1)
	cluster.AddConn(cluster.SelfID(), conn)

	conn2, err := cluster.Conn(cluster.SelfID())
	require.NoError(t, err)
	require.Equal(t, conn, conn2)
}

func TestCluster_Conn_Concurrent(t *testing.T) {
	var dialerCalled int32

	ctrl := gomock.NewController(t)

	clusterConf := membership.DefaultConfig()
	clusterConf.NodeID = 1

	clusterConf.Dialer = func(ctx context.Context, addr string) (nodeapi.Client, error) {
		atomic.AddInt32(&dialerCalled, 1)
		time.Sleep(100 * time.Millisecond) // Simulate membership latency.
		conn := nodeclientmock.NewMockConn(ctrl)
		conn.EXPECT().IsClosed().Return(false).AnyTimes()
		conn.EXPECT().Close().Return(nil).Times(1)
		return conn, nil
	}

	concurrency := 10
	errs := make([]error, concurrency)
	connections := make([]nodeapi.Client, concurrency)

	cluster := membership.NewSWIM(clusterConf)
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
