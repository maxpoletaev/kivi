package service

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kiwi/internal/grpcutil"
	"github.com/maxpoletaev/kiwi/internal/vclock"
	"github.com/maxpoletaev/kiwi/storage"
	storagemock "github.com/maxpoletaev/kiwi/storage/mock"
	"github.com/maxpoletaev/kiwi/storage/proto"
)

func TestGet(t *testing.T) {
	type test struct {
		request        *proto.GetRequest
		setupBackend   func(b *storagemock.MockEngine)
		assertResponse func(t *testing.T, res *proto.GetResponse, err error)
	}

	tests := map[string]test{
		"FoundSingleValue": {
			setupBackend: func(b *storagemock.MockEngine) {
				b.EXPECT().Get("key").Return([]storage.Value{
					{
						Version: vclock.New(vclock.V{1: 1}),
						Data:    []byte("value"),
					},
				}, nil)
			},
			request: &proto.GetRequest{Key: "key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.NoError(t, err)
				assert.Equal(t, 1, len(res.Value))
				assert.Equal(t, []byte("value"), res.Value[0].Data)
				assert.Equal(t, vclock.New(vclock.V{1: 1}), vclock.MustDecode(res.Value[0].Version))
			},
		},
		"FoundMultipleValues": {
			setupBackend: func(b *storagemock.MockEngine) {
				b.EXPECT().Get("key").Return(
					[]storage.Value{
						{
							Version: vclock.New(vclock.V{1: 1}),
							Data:    []byte("value 1"),
						},
						{
							Version: vclock.New(vclock.V{2: 1}),
							Data:    []byte("value 2"),
						},
					}, nil,
				)
			},
			request: &proto.GetRequest{Key: "key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.NoError(t, err)
				assert.Equal(t, 2, len(res.Value))
				assert.Equal(t, []byte("value 1"), res.Value[0].Data)
				assert.Equal(t, vclock.New(vclock.V{1: 1}), vclock.MustDecode(res.Value[0].Version))
				assert.Equal(t, []byte("value 2"), res.Value[1].Data)
				assert.Equal(t, vclock.New(vclock.V{2: 1}), vclock.MustDecode(res.Value[1].Version))
			},
		},
		"NotFound": {
			setupBackend: func(b *storagemock.MockEngine) {
				b.EXPECT().Get("non-existing-key").Return(nil, storage.ErrNotFound)
			},
			request: &proto.GetRequest{Key: "non-existing-key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.NoError(t, err)
				assert.Equal(t, 0, len(res.Value))
			},
		},
		"BackendError": {
			setupBackend: func(b *storagemock.MockEngine) {
				b.EXPECT().Get("key").Return(nil, assert.AnError)
			},
			request: &proto.GetRequest{Key: "key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.Error(t, err)
				code := grpcutil.ErrorCode(err)
				assert.Equal(t, codes.Internal, code)
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			backend := storagemock.NewMockEngine(ctrl)
			service := New(backend, 0)
			ctx := context.Background()

			tt.setupBackend(backend)

			res, err := service.Get(ctx, tt.request)

			tt.assertResponse(t, res, err)
		})
	}
}
