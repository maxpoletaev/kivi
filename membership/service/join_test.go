package service

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kv/internal/grpcutil"
	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
)

func TestJoin(t *testing.T) {
	ctrl := gomock.NewController(t)
	memberRepo := NewMockMemberRegistry(ctrl)
	svc := NewMembershipService(memberRepo)

	// Members to be added to the local cluster.
	memberRepo.EXPECT().Add(
		membership.Member{
			ID:         1,
			Name:       "node1",
			ServerAddr: "192.168.1.1:4000",
			GossipAddr: "192.168.1.1:5000",
			Status:     membership.StatusHealthy,
			Version:    1,
		},
	).Return(nil)

	// Members to be added to the remote cluster.
	memberRepo.EXPECT().Members().Return([]membership.Member{
		{
			ID:         2,
			Name:       "node2",
			ServerAddr: "192.168.1.2:4000",
			GossipAddr: "192.168.1.2:5000",
			Status:     membership.StatusHealthy,
			Version:    1,
		},
	})

	resp, err := svc.Join(context.Background(), &proto.JoinRequest{
		LocalMembers: []*proto.Member{
			{
				Id:         1,
				Name:       "node1",
				ServerAddr: "192.168.1.1:4000",
				GossipAddr: "192.168.1.1:5000",
				Status:     proto.Status_Healthy,
				Version:    1,
			},
		},
	})

	require.NoError(t, err)

	require.Equal(t, &proto.JoinResponse{
		RemoteMembers: []*proto.Member{
			{
				Id:         2,
				Name:       "node2",
				ServerAddr: "192.168.1.2:4000",
				GossipAddr: "192.168.1.2:5000",
				Status:     proto.Status_Healthy,
				Version:    1,
			},
		},
	}, resp)
}

func TestJoinFails_InvalidRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	memberRepo := NewMockMemberRegistry(ctrl)
	svc := NewMembershipService(memberRepo)

	_, err := svc.Join(context.Background(), nil)

	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, grpcutil.ErrorCode(err))
}

func TestJoinFails_AddError(t *testing.T) {
	ctrl := gomock.NewController(t)
	memberRepo := NewMockMemberRegistry(ctrl)
	svc := NewMembershipService(memberRepo)

	memberRepo.EXPECT().Add(gomock.Any()).Return(assert.AnError)
	memberRepo.EXPECT().Members().Return([]membership.Member{})

	_, err := svc.Join(context.Background(), &proto.JoinRequest{
		LocalMembers: []*proto.Member{
			{
				Id:         1,
				Name:       "node1",
				ServerAddr: "192.168.1.1:4000",
				GossipAddr: "192.168.1.1:5000",
				Status:     proto.Status_Healthy,
				Version:    1,
			},
		},
	})

	require.Equal(t, codes.Internal, grpcutil.ErrorCode(err))
	require.ErrorContains(t, err, assert.AnError.Error())
}
