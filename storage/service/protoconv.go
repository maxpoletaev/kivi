package service

import (
	"github.com/maxpoletaev/kiwi/internal/vclock"
	"github.com/maxpoletaev/kiwi/storage"
	"github.com/maxpoletaev/kiwi/storage/proto"
)

func toProtoValues(values []storage.Value) []*proto.VersionedValue {
	versionedValues := make(
		[]*proto.VersionedValue, 0, len(values),
	)

	for _, value := range values {
		versionedValues = append(versionedValues, &proto.VersionedValue{
			Version:   vclock.MustEncode(value.Version),
			Tombstone: value.Tombstone,
			Data:      value.Data,
		})
	}

	return versionedValues
}
