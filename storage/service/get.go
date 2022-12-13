package service

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maxpoletaev/kv/storage"
	"github.com/maxpoletaev/kv/storage/proto"
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
		Value: toResponseValues(values),
	}

	return resp, nil
}

func toResponseValues(values []storage.StoredValue) []*proto.VersionedValue {
	versionedValues := make(
		[]*proto.VersionedValue, 0, len(values),
	)

	for _, value := range values {
		versionedValues = append(versionedValues, &proto.VersionedValue{
			Version: value.Version,
			Data:    value.Blob,
		})
	}

	return versionedValues
}
