package service

import (
	"github.com/maxpoletaev/kv/network"
	"github.com/maxpoletaev/kv/network/proto"
)

type ClusterService struct {
	proto.UnimplementedClusterSericeServer

	cluster *network.Cluster
}

func NewClusterService(cluster *network.Cluster) *ClusterService {
	return &ClusterService{
		cluster: cluster,
	}
}

// func toProtoStatus(s network.Status) proto.Status {
// 	switch s {
// 	case network.StatusAlive:
// 		return proto.Status_Alive
// 	case network.StatusSuspect:
// 		return proto.Status_Suspect
// 	case network.StatusDead:
// 		return proto.Status_Dead
// 	case network.StatusLeft:
// 		return proto.Status_Left
// 	default:
// 		panic("toProtoStatus: unknown status")
// 	}
// }
