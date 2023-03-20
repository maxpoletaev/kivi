package membership_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/nodeapi"
	nodeapimock "github.com/maxpoletaev/kiwi/nodeapi/mock"
)

func TestCluster_Conn(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := nodeapimock.NewMockClient(ctrl)
	conn.EXPECT().Close().Return(nil).Times(1)

	var dialerCalled int

	conf := membership.DefaultConfig()
	conf.Dialer = func(ctx context.Context, addr string) (nodeapi.Client, error) {
		dialerCalled++
		return conn, nil
	}

	cluster := membership.NewCluster(membership.Node{}, conf)
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

	cluster := membership.NewCluster(membership.Node{ID: 1}, conf)
	defer cluster.Leave(context.Background())

	ctrl := gomock.NewController(t)
	conn := nodeapimock.NewMockClient(ctrl)
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
	clusterConf.Dialer = func(ctx context.Context, addr string) (nodeapi.Client, error) {
		atomic.AddInt32(&dialerCalled, 1)
		time.Sleep(100 * time.Millisecond) // Simulate membership latency.
		conn := nodeapimock.NewMockClient(ctrl)
		conn.EXPECT().IsClosed().Return(false).AnyTimes()
		conn.EXPECT().Close().Return(nil).Times(1)
		return conn, nil
	}

	concurrency := 10
	errs := make([]error, concurrency)
	connections := make([]nodeapi.Client, concurrency)

	cluster := membership.NewCluster(membership.Node{ID: 1}, clusterConf)
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
