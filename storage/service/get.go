package service

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kiwi/storage"
	"github.com/maxpoletaev/kiwi/storage/proto"
)

func (s *StorageService) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	values, err := s.storage.Get(req.Key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &proto.GetResponse{}, nil
		}

		return nil, status.New(
			codes.Internal, fmt.Sprintf("storage get failed: %s", err),
		).Err()
	}

	resp := &proto.GetResponse{
		Value: toProtoValues(values),
	}

	return resp, nil
}
