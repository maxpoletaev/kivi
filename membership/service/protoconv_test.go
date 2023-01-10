package service

import (
	"testing"

	"github.com/maxpoletaev/kv/membership"
	"github.com/maxpoletaev/kv/membership/proto"
	"github.com/stretchr/testify/assert"
)

func TestToProtoStatus(t *testing.T) {
	assert.Equal(t, proto.Status_Healthy, toProtoStatus(membership.StatusHealthy))
	assert.Equal(t, proto.Status_Faulty, toProtoStatus(membership.StatusFaulty))
	assert.Panics(t, func() { toProtoStatus(membership.Status(-1)) })
}

func TestToProtoMember(t *testing.T) {
	m := &membership.Member{
		ID:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     membership.StatusHealthy,
	}

	assert.Equal(t, &proto.Member{
		Id:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     proto.Status_Healthy,
	}, toProtoMember(m))
}

func TestToProtoMembers(t *testing.T) {
	members := []membership.Member{
		{
			ID:         1,
			Name:       "node1",
			GossipAddr: "127.0.0.1:4000",
			ServerAddr: "127.0.0.1:4001",
			Version:    1,
			Status:     membership.StatusHealthy,
		},
	}

	result := toProtoMembers(members)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, &proto.Member{
		Id:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     proto.Status_Healthy,
	}, result[0])
}

func TestFromProtoStatus(t *testing.T) {
	assert.Equal(t, membership.StatusHealthy, fromProtoStatus(proto.Status_Healthy))
	assert.Equal(t, membership.StatusFaulty, fromProtoStatus(proto.Status_Faulty))
	assert.Panics(t, func() { fromProtoStatus(proto.Status(-1)) })
}

func TestFromProtoMember(t *testing.T) {
	m := &proto.Member{
		Id:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     proto.Status_Healthy,
	}

	assert.Equal(t, membership.Member{
		ID:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     membership.StatusHealthy,
	}, fromProtoMember(m))
}

func TestFromProtoMembers(t *testing.T) {
	members := []*proto.Member{
		{
			Id:         1,
			Name:       "node1",
			GossipAddr: "127.0.0.1:4000",
			ServerAddr: "127.0.0.1:4001",
			Version:    1,
			Status:     proto.Status_Healthy,
		},
	}

	result := fromProtoMembers(members)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, membership.Member{
		ID:         1,
		Name:       "node1",
		GossipAddr: "127.0.0.1:4000",
		ServerAddr: "127.0.0.1:4001",
		Version:    1,
		Status:     membership.StatusHealthy,
	}, result[0])
}
