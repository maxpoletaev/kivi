package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/internal/vclock"
	"github.com/maxpoletaev/kivi/storage"
	"github.com/maxpoletaev/kivi/storage/mock"
	"github.com/maxpoletaev/kivi/storage/proto"
)

func TestGet(t *testing.T) {
	type test struct {
		request        *proto.GetRequest
		setupBackend   func(b *mock.MockEngine)
		assertResponse func(t *testing.T, res *proto.GetResponse, err error)
	}

	tests := map[string]test{
		"FoundSingleValue": {
			setupBackend: func(b *mock.MockEngine) {
				b.EXPECT().Get("key").Return([]storage.Value{
					{
						Version: vclock.Version{1: 1},
						Data:    []byte("value"),
					},
				}, nil)
			},
			request: &proto.GetRequest{Key: "key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.NoError(t, err)
				assert.Equal(t, 1, len(res.Value))
				assert.Equal(t, []byte("value"), res.Value[0].Data)
				assert.Equal(t, vclock.Version{1: 1}, vclock.MustFromString(res.Value[0].Version))
			},
		},
		"FoundMultipleValues": {
			setupBackend: func(b *mock.MockEngine) {
				b.EXPECT().Get("key").Return(
					[]storage.Value{
						{
							Version: vclock.Version{1: 1},
							Data:    []byte("value 1"),
						},
						{
							Version: vclock.Version{2: 1},
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
				assert.Equal(t, vclock.Version{1: 1}, vclock.MustFromString(res.Value[0].Version))
				assert.Equal(t, []byte("value 2"), res.Value[1].Data)
				assert.Equal(t, vclock.Version{2: 1}, vclock.MustFromString(res.Value[1].Version))
			},
		},
		"NotFound": {
			setupBackend: func(b *mock.MockEngine) {
				b.EXPECT().Get("non-existing-key").Return(nil, storage.ErrNotFound)
			},
			request: &proto.GetRequest{Key: "non-existing-key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.NoError(t, err)
				assert.Equal(t, 0, len(res.Value))
			},
		},
		"BackendError": {
			setupBackend: func(b *mock.MockEngine) {
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
			backend := mock.NewMockEngine(ctrl)
			service := New(backend, 0)
			ctx := context.Background()

			tt.setupBackend(backend)

			res, err := service.Get(ctx, tt.request)

			tt.assertResponse(t, res, err)
		})
	}
}

func TestPut(t *testing.T) {
	type test struct {
		setupBackend   func(backend *mock.MockEngine)
		request        *proto.PutRequest
		assertResponse func(t *testing.T, res *proto.PutResponse, err error)
	}

	tests := map[string]test{
		"OkPrimary": {
			setupBackend: func(b *mock.MockEngine) {
				b.EXPECT().Put("key", storage.Value{
					Version: vclock.Version{100: 2, 200: 1},
					Data:    []byte("value"),
				}).Return(nil)
			},
			request: &proto.PutRequest{
				Key:     "key",
				Primary: true,
				Value: &proto.VersionedValue{
					Version: vclock.ToString(vclock.Version{100: 1, 200: 1}),
					Data:    []byte("value"),
				},
			},
			assertResponse: func(t *testing.T, res *proto.PutResponse, err error) {
				require.NoError(t, err)

				version := vclock.Version{100: 2, 200: 1}
				assert.Equal(t, version, vclock.MustFromString(res.Version))
			},
		},
		"OkNonPrimary": {
			setupBackend: func(b *mock.MockEngine) {
				b.EXPECT().Put("key", storage.Value{
					Version: vclock.Version{100: 1, 200: 1},
					Data:    []byte("value"),
				}).Return(nil)
			},
			request: &proto.PutRequest{
				Key:     "key",
				Primary: false,
				Value: &proto.VersionedValue{
					Version: vclock.ToString(vclock.Version{100: 1, 200: 1}),
					Data:    []byte("value"),
				},
			},
			assertResponse: func(t *testing.T, res *proto.PutResponse, err error) {
				require.NoError(t, err)

				version := vclock.Version{100: 1, 200: 1}
				assert.Equal(t, version, vclock.MustFromString(res.Version))
			},
		},
		"FailsObsoleteWrite": {
			setupBackend: func(b *mock.MockEngine) {
				b.EXPECT().Put("key", storage.Value{
					Version: vclock.Empty(),
					Data:    []byte{},
				}).Return(storage.ErrObsolete)
			},
			request: &proto.PutRequest{
				Key: "key",
				Value: &proto.VersionedValue{
					Version: vclock.ToString(vclock.Empty()),
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
			setupBackend: func(b *mock.MockEngine) {
				b.EXPECT().Put("key", storage.Value{
					Version: vclock.Empty(),
					Data:    []byte{},
				}).Return(assert.AnError)
			},
			request: &proto.PutRequest{
				Key: "key",
				Value: &proto.VersionedValue{
					Version: vclock.ToString(vclock.Empty()),
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
			backend := mock.NewMockEngine(ctrl)
			service := New(backend, 100)
			ctx := context.Background()

			tt.setupBackend(backend)

			res, err := service.Put(ctx, tt.request)

			tt.assertResponse(t, res, err)
		})
	}
}
