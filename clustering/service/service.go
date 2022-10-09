package service

import (
	"github.com/maxpoletaev/kv/clustering"
	"github.com/maxpoletaev/kv/clustering/proto"
)

type cluster interface {
	Nodes() []clustering.Node
	AddMembers(nodes []clustering.Member) error
	NodeByID(clustering.NodeID) *clustering.Node
}

type ClusterService struct {
	proto.UnimplementedClusteringSericeServer

	cluster cluster
}

func NewClusteringService(cluster cluster) *ClusterService {
	return &ClusterService{
		cluster: cluster,
	}
}
