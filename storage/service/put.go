package service

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/storage"
	"github.com/maxpoletaev/kv/storage/proto"
)

func (s *StorageService) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	version := vclock.Vector(req.Value.Version)

	if version == nil {
		version = make(vclock.Vector)
	}

	if req.Primary {
		version.Increment(s.nodeID)
	}

	value := storage.StoredValue{
		Blob:    req.Value.Data,
		Version: version,
	}

	err := s.storage.Put(req.Key, value)
	if err != nil {
		if errors.Is(err, storage.ErrObsoleteWrite) {
			return nil, status.New(codes.AlreadyExists, "obsolete write").Err()
		}

		return nil, status.New(
			codes.Internal, fmt.Sprintf("storage put failed: %s", err),
		).Err()
	}

	return &proto.PutResponse{
		Version: version,
	}, nil
}
