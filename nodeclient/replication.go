package nodeclient

import (
	"context"
	"errors"
)

var (
	ErrVersionConflict = errors.New("version conflict")
)

type RepGetResponse struct {
	Values  [][]byte
	Version string
}

type replicationClient interface {
	// RepGet returns the value of the key and the version of the key.
	RepGet(ctx context.Context, key string) (*RepGetResponse, error)

	// RepPut puts the value of the key and returns the new version of the key.
	RepPut(ctx context.Context, key string, value []byte, version string) (string, error)

	// RepDelete deletes the value associated with the key and returns the new version of the key.
	RepDelete(ctx context.Context, key string, version string) (string, error)
}
