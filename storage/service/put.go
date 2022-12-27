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
	version, err := vclock.Decode(req.Value.Version)
	if err != nil {
		return nil, status.New(
			codes.InvalidArgument, fmt.Sprintf("invalid version: %s", err),
		).Err()
	}

	if req.Primary {
		version.Update(s.nodeID)
	}

	value := storage.Value{
		Data:    req.Value.Data,
		Version: version,
	}

	err = s.storage.Put(req.Key, value)
	if err != nil {
		if errors.Is(err, storage.ErrObsoleteWrite) {
			return nil, status.New(codes.AlreadyExists, "obsolete write").Err()
		}

		return nil, status.New(
			codes.Internal, fmt.Sprintf("storage put failed: %s", err),
		).Err()
	}

	return &proto.PutResponse{
		Version: vclock.MustEncode(version),
	}, nil
}
