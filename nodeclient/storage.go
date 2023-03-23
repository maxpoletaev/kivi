package nodeclient

import "context"

type VersionedValue struct {
	Version   string
	Data      []byte
	Tombstone bool
}

type PutResponse struct {
	Version string
}

type storageClient interface {
	StorageGet(ctx context.Context, key string) ([]VersionedValue, error)
	StoragePut(ctx context.Context, key string, value VersionedValue, primary bool) (*PutResponse, error)
}
