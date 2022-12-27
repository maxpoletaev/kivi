package engine

import (
	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/storage"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
)

func fromProtoValue(v *proto.Value) storage.Value {
	return storage.Value{
		Version: vclock.MustDecode(v.Version),
		Data:    v.Data,
	}
}

func fromProtoValues(vs []*proto.Value) []storage.Value {
	values := make([]storage.Value, 0, len(vs))

	for _, v := range vs {
		values = append(values, fromProtoValue(v))
	}

	return values
}

func toProtoValue(v storage.Value) *proto.Value {
	return &proto.Value{
		Version: vclock.MustEncode(v.Version),
		Data:    v.Data,
	}
}

func toProtoValues(vs []storage.Value) []*proto.Value {
	values := make([]*proto.Value, 0, len(vs))

	for _, v := range vs {
		values = append(values, toProtoValue(v))
	}

	return values
}
