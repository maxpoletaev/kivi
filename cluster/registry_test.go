package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kv/cluster/mock"
	"github.com/maxpoletaev/kv/membership"
)

func TestRegistry_GetExisting(t *testing.T) {
	ctrl := gomock.NewController(t)
	conn := mock.NewMockClient(ctrl)
	conn.EXPECT().IsClosed().Return(false).AnyTimes()

	memberRepo := NewMockMemberRegistry(ctrl)
	dialer := NewMockDialer(ctrl)
	registry := NewConnRegistry(memberRepo, dialer)
	registry.connections[1] = conn

	got, err := registry.Get(1)
	require.NoError(t, err)

	require.Equal(t, conn, got)
}

func TestRegistry_GetClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	closedConn := mock.NewMockClient(ctrl)
	closedConn.EXPECT().IsClosed().Return(true).Times(2)

	memberRepo := NewMockMemberRegistry(ctrl)
	memberRepo.EXPECT().Member(membership.NodeID(1)).Return(
		membership.Member{ID: 1, Name: "node1", ServerAddr: "192.168.10.1:8000"}, true,
	)

	dialer := NewMockDialer(ctrl)
	conn := mock.NewMockClient(ctrl)
	dialer.EXPECT().DialContext(gomock.Any(), "192.168.10.1:8000").Return(conn, nil)

	registry := NewConnRegistry(memberRepo, dialer)
	registry.connections[1] = closedConn

	got, err := registry.Get(1)
	require.NoError(t, err)
	require.Equal(t, conn, got)
}

func TestRegistry_GetNew(t *testing.T) {
	ctrl := gomock.NewController(t)

	memberRepo := NewMockMemberRegistry(ctrl)
	memberRepo.EXPECT().Member(membership.NodeID(1)).Return(membership.Member{
		ID:         1,
		Name:       "node1",
		ServerAddr: "192.168.10.1:8000",
	}, true)

	dialer := NewMockDialer(ctrl)
	conn := mock.NewMockClient(ctrl)
	dialer.EXPECT().DialContext(gomock.Any(), "192.168.10.1:8000").Return(conn, nil)

	registry := NewConnRegistry(memberRepo, dialer)
	got, err := registry.Get(1)

	require.NoError(t, err)
	require.Equal(t, conn, got)
}

func TestRegistry_GetConcurrent(t *testing.T) {
	ctrl := gomock.NewController(t)

	member := membership.Member{
		ID:         1,
		Name:       "node1",
		ServerAddr: "192.168.10.1:8000",
	}

	memberRepo := NewMockMemberRegistry(ctrl)
	memberRepo.EXPECT().Member(membership.NodeID(1)).Return(member, true).MinTimes(1)

	dialer := NewMockDialer(ctrl)
	dialer.EXPECT().DialContext(
		gomock.Any(), "192.168.10.1:8000",
	).DoAndReturn(func(ctx context.Context, addr string) (Client, error) {
		time.Sleep(100 * time.Millisecond) // Simulate network latency.
		conn := mock.NewMockClient(ctrl)
		conn.EXPECT().IsClosed().Return(false).AnyTimes()
		return conn, nil
	}).Times(1)

	concurrency := 10
	registry := NewConnRegistry(memberRepo, dialer)
	connections := make([]Client, concurrency)
	errs := make([]error, concurrency)

	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	begin := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			<-begin

			conn, err := registry.Get(1)
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
}
