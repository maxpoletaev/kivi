package service

import (
	"github.com/maxpoletaev/kiwi/storage"
	"github.com/maxpoletaev/kiwi/storage/proto"
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
