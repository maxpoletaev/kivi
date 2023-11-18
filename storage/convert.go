package storage

import (
	"github.com/maxpoletaev/kivi/internal/vclock"
	"github.com/maxpoletaev/kivi/storage/proto"
)

func ToProtoValues(values []Value) []*proto.VersionedValue {
	versionedValues := make(
		[]*proto.VersionedValue, 0, len(values),
	)

	for _, value := range values {
		versionedValues = append(versionedValues, &proto.VersionedValue{
			Version:   vclock.ToString(value.Version),
			Tombstone: value.Tombstone,
			Data:      value.Data,
		})
	}

	return versionedValues
}
