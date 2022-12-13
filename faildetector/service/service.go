package service

import (
	"github.com/maxpoletaev/kv/cluster"
	"github.com/maxpoletaev/kv/faildetector/proto"
	"github.com/maxpoletaev/kv/membership"
)

type Cluster interface {
	Conn(id membership.NodeID) (cluster.Client, error)
	HasMember(id membership.NodeID) bool
	Self() membership.Member
}

type FailDetectorService struct {
	proto.UnimplementedFailDetectorServiceServer
	cluster Cluster
}

func New(cluster Cluster) *FailDetectorService {
	return &FailDetectorService{
		cluster: cluster,
	}
}
