package storage

//go:generate mockgen -source=storage.go -destination=mock/storage_mock.go -package=mock

import (
	"errors"

	"github.com/maxpoletaev/kv/internal/vclock"
)

var (
	ErrObsoleteWrite = errors.New("obsolete write")
	ErrNotFound      = errors.New("key not found")
	ErrNoMoreItems   = errors.New("no more items in the iterator")
)

type StoredValue struct {
	Version vclock.Vector
	Blob    []byte
}

type Backend interface {
	Get(key string) ([]StoredValue, error)
	Put(key string, value StoredValue) error
}

type ScanIterator interface {
	Next() (key string, value StoredValue)
	HasNext() bool
}

func AppendVersion(values []StoredValue, newValue StoredValue) ([]StoredValue, error) {
	merged := make([]StoredValue, 0, len(values)+1)

	for _, val := range values {
		switch vclock.Compare(newValue.Version, val.Version) {
		case vclock.Before, vclock.Equal:
			return nil, ErrObsoleteWrite
		case vclock.Concurrent:
			merged = append(merged, val)
		}
	}

	merged = append(merged, newValue)

	return merged, nil
}
