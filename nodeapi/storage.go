package nodeapi

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
	Get(ctx context.Context, key string) ([]VersionedValue, error)
	Put(ctx context.Context, key string, value VersionedValue, primary bool) (*PutResponse, error)
}
