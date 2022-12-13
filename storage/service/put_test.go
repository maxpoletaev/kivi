package service

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kv/internal/grpcutil"
	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/storage"
	"github.com/maxpoletaev/kv/storage/mock"
	"github.com/maxpoletaev/kv/storage/proto"
)

func TestPut(t *testing.T) {
	type test struct {
		setupBackend   func(backend *mock.MockBackend)
		request        *proto.PutRequest
		assertResponse func(t *testing.T, res *proto.PutResponse, err error)
	}

	tests := map[string]test{
		"OkPrimary": {
			setupBackend: func(b *mock.MockBackend) {
				b.EXPECT().Put("key", storage.StoredValue{
					Version: vclock.New(vclock.V{100: 2, 200: 1}),
					Blob:    []byte("value"),
				}).Return(nil)
			},
			request: &proto.PutRequest{
				Key:     "key",
				Primary: true,
				Value: &proto.VersionedValue{
					Version: vclock.NewEncoded(vclock.V{100: 1, 200: 1}),
					Data:    []byte("value"),
				},
			},
			assertResponse: func(t *testing.T, res *proto.PutResponse, err error) {
				require.NoError(t, err)

				version := vclock.New(vclock.V{100: 2, 200: 1})
				assert.Equal(t, version, vclock.MustDecode(res.Version))
			},
		},
		"OkNonPrimary": {
			setupBackend: func(b *mock.MockBackend) {
				b.EXPECT().Put("key", storage.StoredValue{
					Version: vclock.New(vclock.V{100: 1, 200: 1}),
					Blob:    []byte("value"),
				}).Return(nil)
			},
			request: &proto.PutRequest{
				Key:     "key",
				Primary: false,
				Value: &proto.VersionedValue{
					Version: vclock.NewEncoded(vclock.V{100: 1, 200: 1}),
					Data:    []byte("value"),
				},
			},
			assertResponse: func(t *testing.T, res *proto.PutResponse, err error) {
				require.NoError(t, err)

				version := vclock.New(vclock.V{100: 1, 200: 1})
				assert.Equal(t, version, vclock.MustDecode(res.Version))
			},
		},
		"FailsObsoleteWrite": {
			setupBackend: func(b *mock.MockBackend) {
				b.EXPECT().Put("key", storage.StoredValue{
					Version: vclock.New(),
					Blob:    []byte{},
				}).Return(storage.ErrObsoleteWrite)
			},
			request: &proto.PutRequest{
				Key: "key",
				Value: &proto.VersionedValue{
					Version: vclock.NewEncoded(),
					Data:    []byte{},
				},
			},
			assertResponse: func(t *testing.T, res *proto.PutResponse, err error) {
				require.Error(t, err)
				code := grpcutil.ErrorCode(err)
				assert.Equal(t, codes.AlreadyExists, code)
			},
		},
		"FailsRandomError": {
			setupBackend: func(b *mock.MockBackend) {
				b.EXPECT().Put("key", storage.StoredValue{
					Version: vclock.New(),
					Blob:    []byte{},
				}).Return(assert.AnError)
			},
			request: &proto.PutRequest{
				Key: "key",
				Value: &proto.VersionedValue{
					Version: vclock.NewEncoded(),
					Data:    []byte{},
				},
			},
			assertResponse: func(t *testing.T, res *proto.PutResponse, err error) {
				require.Error(t, err)
				code := grpcutil.ErrorCode(err)
				assert.Equal(t, codes.Internal, code)
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			backend := mock.NewMockBackend(ctrl)
			service := New(backend, 100)
			ctx := context.Background()

			tt.setupBackend(backend)

			res, err := service.Put(ctx, tt.request)

			tt.assertResponse(t, res, err)
		})
	}
}
