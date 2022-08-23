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

func TestPut(t *testing.T) {
	type test struct {
		prepareBackend func(backend *storage.BackendMock)
		request        *proto.PutRequest
		assertResponse func(t *testing.T, res *proto.PutResponse, err error)
	}

	tests := map[string]test{
		"OkPrimary": {
			prepareBackend: func(backend *storage.BackendMock) {
				backend.PutFunc = func(key string, value storage.StoredValue) error {
					return nil
				}
			},
			request: &proto.PutRequest{
				Key:     "key",
				Primary: true,
				Value: &proto.VersionedValue{
					Version: vclock.Vector{100: 1, 200: 1},
				},
			},
			assertResponse: func(t *testing.T, res *proto.PutResponse, err error) {
				require.NoError(t, err)

				version := vclock.Vector{100: 2, 200: 1}
				assert.Equal(t, version, vclock.Vector(res.Version))
			},
		},
		"OkNonPrimary": {
			prepareBackend: func(backend *storage.BackendMock) {
				backend.PutFunc = func(key string, value storage.StoredValue) error {
					return nil
				}
			},
			request: &proto.PutRequest{
				Key:     "key",
				Primary: false,
				Value: &proto.VersionedValue{
					Version: vclock.Vector{100: 1, 200: 1},
				},
			},
			assertResponse: func(t *testing.T, res *proto.PutResponse, err error) {
				require.NoError(t, err)

				version := vclock.Vector{100: 1, 200: 1}
				assert.Equal(t, version, vclock.Vector(res.Version))
			},
		},
		"FailsObsoleteWrite": {
			prepareBackend: func(backend *storage.BackendMock) {
				backend.PutFunc = func(key string, value storage.StoredValue) error {
					return storage.ErrObsoleteWrite
				}
			},
			request: &proto.PutRequest{
				Key: "key",
				Value: &proto.VersionedValue{
					Version: vclock.Vector{},
				},
			},
			assertResponse: func(t *testing.T, res *proto.PutResponse, err error) {
				require.Error(t, err)
				code := grpcutil.ErrorCode(err)
				assert.Equal(t, codes.AlreadyExists, code)
			},
		},
		"FailsRandomError": {
			prepareBackend: func(backend *storage.BackendMock) {
				backend.PutFunc = func(key string, value storage.StoredValue) error {
					return assert.AnError
				}
			},
			request: &proto.PutRequest{
				Key: "key",
				Value: &proto.VersionedValue{
					Version: vclock.Vector{},
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
			backend := &storage.BackendMock{}
			service := New(backend, 100)
			ctx := context.Background()

			tt.prepareBackend(backend)

			res, err := service.Put(ctx, tt.request)

			tt.assertResponse(t, res, err)
		})
	}
}
