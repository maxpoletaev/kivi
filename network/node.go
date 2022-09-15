package network

import "github.com/maxpoletaev/kv/network/proto"

type NodeID uint32

type Node struct {
	ID              NodeID
	Name            string
	ServerAddr      string
	GossipAddr      string
	LocalServerAddr string
}

func ToProtoNode(n *Node) *proto.Node {
	return &proto.Node{
		Id:         uint32(n.ID),
		Name:       n.Name,
		GossipAddr: n.GossipAddr,
		ServerAddr: n.ServerAddr,
	}
}

func FromProtoNode(n *proto.Node) Node {
	return Node{
		ID:         NodeID(n.Id),
		Name:       n.Name,
		GossipAddr: n.GossipAddr,
		ServerAddr: n.ServerAddr,
	}
}
