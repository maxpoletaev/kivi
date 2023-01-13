package service

import (
	"context"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kiwi/internal/grpcutil"
	"github.com/maxpoletaev/kiwi/membership"
	"github.com/maxpoletaev/kiwi/membership/proto"
)

func TestExpell(t *testing.T) {
	ctrl := gomock.NewController(t)
	memberRepo := NewMockMemberRegistry(ctrl)
	svc := NewMembershipService(memberRepo)
	ctx := context.Background()
	memberRepo.EXPECT().Expel(membership.NodeID(1)).Return(nil)
	_, err := svc.Expel(ctx, &proto.ExpelRequest{MemberId: 1})
	assert.NoError(t, err)
}

func TestExpellFails_MemberNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	memberRepo := NewMockMemberRegistry(ctrl)
	svc := NewMembershipService(memberRepo)
	ctx := context.Background()
	memberRepo.EXPECT().Expel(membership.NodeID(1)).Return(membership.ErrMemberNotFound)
	_, err := svc.Expel(ctx, &proto.ExpelRequest{MemberId: 1})
	assert.Equal(t, codes.NotFound, grpcutil.ErrorCode(err))
}
