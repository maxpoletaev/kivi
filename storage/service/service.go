package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/maxpoletaev/kivi/internal/vclock"
	"github.com/maxpoletaev/kivi/storage"
	"github.com/maxpoletaev/kivi/storage/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errNotSupported = status.New(codes.Unimplemented, "not supported").Err()
)

type StorageService struct {
	proto.UnimplementedStorageServer

	storage storage.Engine
	nodeID  uint32
}

func New(s storage.Engine, nodeID uint32) *StorageService {
	return &StorageService{
		storage: s,
		nodeID:  nodeID,
	}
}

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
		Value: storage.ToProtoValues(values),
	}

	return resp, nil
}

func (s *StorageService) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	version, err := vclock.FromString(req.Value.Version)

	if err != nil {
		return nil, status.New(
			codes.InvalidArgument, fmt.Sprintf("invalid version: %s", err),
		).Err()
	}

	if req.Primary {
		version[s.nodeID]++
	}

	value := storage.Value{
		Version:   version,
		Data:      req.Value.Data,
		Tombstone: req.Value.Tombstone,
	}

	err = s.storage.Put(req.Key, value)
	if err != nil {
		if errors.Is(err, storage.ErrObsolete) {
			return nil, status.New(codes.AlreadyExists, "obsolete write").Err()
		}

		return nil, status.New(
			codes.Internal, fmt.Sprintf("storage put failed: %s", err),
		).Err()
	}

	return &proto.PutResponse{
		Version: vclock.ToString(version),
	}, nil
}

func (s *StorageService) Scan(req *proto.ScanRequest, stream proto.Storage_ScanServer) error {
	st, ok := s.storage.(storage.Scannable)
	if !ok {
		return errNotSupported
	}

	it := st.Scan(req.StartKey)

	for {
		if err := it.Next(); err != nil {
			if errors.Is(err, storage.ErrNoMoreItems) {
				return nil
			}

			return err
		}

		key, values := it.Item()
		resp := &proto.ScanResponse{
			Value: storage.ToProtoValues(values),
			Key:   key,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
