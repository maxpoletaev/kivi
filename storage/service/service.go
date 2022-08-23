package service

import (
	"github.com/maxpoletaev/kv/storage"
	"github.com/maxpoletaev/kv/storage/proto"
)

type StorageService struct {
	proto.UnimplementedStorageServiceServer

	storage storage.Backend
	nodeID  uint32
}

func New(s storage.Backend, nodeID uint32) *StorageService {
	return &StorageService{
		storage: s,
		nodeID:  nodeID,
	}
}
