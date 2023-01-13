package service

import (
	"github.com/maxpoletaev/kiwi/faildetector/proto"
)

type FailDetectorService struct {
	proto.UnimplementedFailDetectorServiceServer
	connections ConnRegistry
	members     Memberlist
}

func New(members Memberlist, connections ConnRegistry) *FailDetectorService {
	return &FailDetectorService{
		connections: connections,
		members:     members,
	}
}
