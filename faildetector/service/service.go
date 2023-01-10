package service

import (
	"github.com/maxpoletaev/kv/faildetector/proto"
)

type FailDetectorService struct {
	proto.UnimplementedFailDetectorServiceServer
	cluster Cluster
}

func New(cluster Cluster) *FailDetectorService {
	return &FailDetectorService{
		cluster: cluster,
	}
}
