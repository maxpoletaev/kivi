package nodeapi

import "context"

type VersionedValue struct {
	Version   string
	Data      []byte
	Tombstone bool
}

type StorageGetResult struct {
	Versions []VersionedValue
}

type StoragePutResult struct {
	Version string
}

type storageClient interface {
	StorageGet(ctx context.Context, key string) (*StorageGetResult, error)
	StoragePut(ctx context.Context, key string, value VersionedValue, primary bool) (*StoragePutResult, error)
}
