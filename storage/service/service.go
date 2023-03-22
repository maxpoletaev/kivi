package service

import (
	"github.com/maxpoletaev/kivi/storage"
	"github.com/maxpoletaev/kivi/storage/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errNotSupported = status.New(codes.Unimplemented, "not supported").Err()
)

type StorageService struct {
	proto.UnimplementedStorageServiceServer

	storage storage.Engine
	nodeID  uint32
}

func New(s storage.Engine, nodeID uint32) *StorageService {
	return &StorageService{
		storage: s,
		nodeID:  nodeID,
	}
}
