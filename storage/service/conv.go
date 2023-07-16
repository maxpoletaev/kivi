package service

import (
	"github.com/maxpoletaev/kivi/internal/vclock"
	"github.com/maxpoletaev/kivi/storage"
	"github.com/maxpoletaev/kivi/storage/proto"
)

func toProtoValues(values []storage.Value) []*proto.VersionedValue {
	versionedValues := make(
		[]*proto.VersionedValue, 0, len(values),
	)

	for _, value := range values {
		versionedValues = append(versionedValues, &proto.VersionedValue{
			Version:   vclock.Encode(value.Version),
			Tombstone: value.Tombstone,
			Data:      value.Data,
		})
	}

	return versionedValues
}
