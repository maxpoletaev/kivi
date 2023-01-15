package storage

//go:generate mockgen -source=storage.go -destination=mock/storage_mock.go -package=mock

import (
	"errors"

	"github.com/maxpoletaev/kiwi/internal/vclock"
)

var (
	// ErrNotFound is returned when a key is not found in the storage.
	ErrNotFound = errors.New("key not found")

	// ErrObsoleteWrite is returned when a write operation is
	// performed on a key that already has a newer version.
	ErrObsoleteWrite = errors.New("obsolete write")

	// ErrNoMoreItems is returned when there are no more items in the iterator.
	ErrNoMoreItems = errors.New("no more items in the iterator")
)

// Value represents a single value associated with a key.
type Value struct {
	Version   *vclock.Vector
	Data      []byte
	Tombstone bool
}

// Engine is the interface that wraps the basic storage operations. It is implemented by
// different storage engines, such as LSM-tree or in-memory storage, and can be easily
// swapped out. Not all storage engines may support all operations, so the interface is
// intentionally small. Note that in case of concurrent versions, the storage engine
// will return all versions of the key, and it is up to the caller to decide which one
// to use.
type Engine interface {
	Get(key string) ([]Value, error)
	Put(key string, value Value) error
}

// Scannable is a storage that supports range scans. It may be supported by some storage
// engines, but not all of them. In case of concurrent versions, the storage engine will
// return all versions of the key, and it is up to the caller to decide which one to use.
type Scannable interface {
	Scan() ScanIterator
	ScanFrom(key string) ScanIterator
	ScanTo(key string) ScanIterator
	ScanRange(from, to string) ScanIterator
}

// ScanIterator is the interface for iterating over the key-value pairs in the storage,
// in lexicographical order. It is not usually safe for concurrent use, so we must create
// a new iterator for each goroutine.
type ScanIterator interface {
	Next() (key string, value Value)
	HasNext() bool
}

// AppendVersion appends a new version to the list of versions. In case the new version
// overtakes the existing ones, the older existing versions are discarded. If the new
// version is older than the existing ones, an ErrObsoleteWrite is returned. In case
// of concurrent versions, the new version is added to the list.
func AppendVersion(values []Value, newValue Value) ([]Value, error) {
	merged := make([]Value, 0, 1)

	for _, val := range values {
		switch vclock.Compare(newValue.Version, val.Version) {
		case vclock.Before, vclock.Equal:
			return nil, ErrObsoleteWrite

		case vclock.Concurrent:
			if !val.Tombstone {
				merged = append(merged, val)
			}
		}
	}

	if len(merged) > 0 && newValue.Tombstone {
		return nil, ErrObsoleteWrite
	}

	merged = append(merged, newValue)

	return merged, nil
}
