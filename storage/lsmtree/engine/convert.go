package engine

import (
	"github.com/maxpoletaev/kivi/internal/vclock"
	"github.com/maxpoletaev/kivi/storage"
	"github.com/maxpoletaev/kivi/storage/lsmtree/proto"
)

func fromProtoValue(v *proto.Value) storage.Value {
	return storage.Value{
		Version:   vclock.MustFromString(v.Version),
		Tombstone: v.Tombstone,
		Data:      v.Data,
	}
}

func fromProtoValueList(vs []*proto.Value) []storage.Value {
	values := make([]storage.Value, 0, len(vs))

	for _, v := range vs {
		values = append(values, fromProtoValue(v))
	}

	return values
}

func toProtoValue(v storage.Value) *proto.Value {
	return &proto.Value{
		Version:   vclock.ToString(v.Version),
		Tombstone: v.Tombstone,
		Data:      v.Data,
	}
}

func toProtoValueList(vs []storage.Value) []*proto.Value {
	values := make([]*proto.Value, 0, len(vs))

	for _, v := range vs {
		values = append(values, toProtoValue(v))
	}

	return values
}
