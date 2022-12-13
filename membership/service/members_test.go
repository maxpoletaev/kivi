package service

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	memberlist := NewMockMemberRegistry(ctrl)

	memberlist.EXPECT().Members().Return([]membership.Member{
		{
			ID:         1,
			Name:       "node1",
			ServerAddr: "127.0.0.1:4000",
			GossipAddr: "127.0.0.1:4001",
			Status:     membership.StatusHealthy,
			Version:    1,
		},
	})

	ctx := context.Background()
	svc := NewMembershipService(memberlist)

	resp, err := svc.Members(ctx, &emptypb.Empty{})

	require.NoError(t, err)
	require.Len(t, resp.Members, 1)

	require.Equal(t, uint32(1), resp.Members[0].Id)
	require.Equal(t, "node1", resp.Members[0].Name)
	require.Equal(t, "127.0.0.1:4000", resp.Members[0].ServerAddr)
	require.Equal(t, "127.0.0.1:4001", resp.Members[0].GossipAddr)
	require.Equal(t, proto.Status_Healthy, resp.Members[0].Status)
	require.Equal(t, uint64(1), resp.Members[0].Version)
}
