package nodeapi

import (
	"context"
	"errors"
)

var (
	ErrVersionConflict = errors.New("version conflict")
)

type GetKeyResult struct {
	Values       [][]byte
	Version      string
	Acknowledged int
}

type PutKeyResult struct {
	Version      string
	Acknowledged int
}

type DeleteKeyResult struct {
	Version      string
	Acknowledged int
}

type replicationClient interface {
	// GetKey returns the value of the key and the version of the key.
	GetKey(ctx context.Context, key string) (*GetKeyResult, error)
	// PutKey puts the value of the key and returns the new version of the key.
	PutKey(ctx context.Context, key string, value []byte, version string) (*PutKeyResult, error)
	// DeleteKey deletes the value associated with the key and returns the new version of the key.
	DeleteKey(ctx context.Context, key string, version string) (*DeleteKeyResult, error)
}
