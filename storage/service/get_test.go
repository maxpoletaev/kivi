package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kv/internal/grpcutil"
	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/storage"
	"github.com/maxpoletaev/kv/storage/proto"
)

func TestGet(t *testing.T) {
	type test struct {
		prepareBackend func(backend *storage.BackendMock)
		request        *proto.GetRequest
		assertResponse func(t *testing.T, res *proto.GetResponse, err error)
	}

	tests := map[string]test{
		"FoundSingleValue": {
			prepareBackend: func(backend *storage.BackendMock) {
				backend.GetFunc = func(key string) ([]storage.StoredValue, error) {
					return []storage.StoredValue{
						{
							Version: vclock.Vector{1: 1},
							Blob:    []byte("value"),
						},
					}, nil
				}
			},
			request: &proto.GetRequest{Key: "key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.NoError(t, err)
				assert.Equal(t, true, res.Found)
				assert.Equal(t, 1, len(res.Value))
				assert.Equal(t, []byte("value"), res.Value[0].Data)
				assert.Equal(t, map[uint32]uint64{1: 1}, res.Value[0].Version)
			},
		},
		"FoundMultipleValues": {
			prepareBackend: func(backend *storage.BackendMock) {
				backend.GetFunc = func(key string) ([]storage.StoredValue, error) {
					return []storage.StoredValue{
						{
							Version: vclock.Vector{1: 1},
							Blob:    []byte("value 1"),
						},
						{
							Version: vclock.Vector{2: 1},
							Blob:    []byte("value 2"),
						},
					}, nil
				}
			},
			request: &proto.GetRequest{Key: "key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.NoError(t, err)
				assert.Equal(t, true, res.Found)
				assert.Equal(t, 2, len(res.Value))
				assert.Equal(t, []byte("value 1"), res.Value[0].Data)
				assert.Equal(t, map[uint32]uint64{1: 1}, res.Value[0].Version)
				assert.Equal(t, []byte("value 2"), res.Value[1].Data)
				assert.Equal(t, map[uint32]uint64{2: 1}, res.Value[1].Version)
			},
		},
		"NotFound": {
			prepareBackend: func(backend *storage.BackendMock) {
				backend.GetFunc = func(key string) ([]storage.StoredValue, error) {
					return nil, storage.ErrNotFound
				}
			},
			request: &proto.GetRequest{Key: "non-existing-key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.NoError(t, err)
				assert.False(t, res.Found)
			},
		},
		"BackendError": {
			prepareBackend: func(backend *storage.BackendMock) {
				backend.GetFunc = func(key string) ([]storage.StoredValue, error) {
					return nil, assert.AnError
				}
			},
			request: &proto.GetRequest{Key: "non-existing-key"},
			assertResponse: func(t *testing.T, res *proto.GetResponse, err error) {
				require.Error(t, err)
				code := grpcutil.ErrorCode(err)
				assert.Equal(t, codes.Internal, code)
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			backend := &storage.BackendMock{}
			service := New(backend, 0)
			ctx := context.Background()

			tt.prepareBackend(backend)

			res, err := service.Get(ctx, tt.request)

			tt.assertResponse(t, res, err)
		})
	}
}
