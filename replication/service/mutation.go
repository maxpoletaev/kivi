package service

import (
	"context"
	"time"

	kitlog "github.com/go-kit/log"
	"google.golang.org/grpc/codes"

	"github.com/maxpoletaev/kivi/internal/grpcutil"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/replication"
	"github.com/maxpoletaev/kivi/replication/consistency"
)

const (
	maxRetries = 3
)

type dataType[T any] interface {
	FromBytes(state []byte) error
	ToBytes() ([]byte, error)
	Merge(other T)
	Modified() bool
}

type actionResult[T any] struct {
	version      string
	conflict     bool
	acknowledged int
	value        T
}

type typeMutation[T dataType[T]] struct {
	New     func() T
	RetryOn []codes.Code

	cluster membership.Cluster
	level   consistency.Level
	logger  kitlog.Logger
	timeout time.Duration
}

// Do runs the mutation function on the value stored at the given key. It first
// fetches the value from the storage and decodes it into the appropriate type.
// Then it runs the mutation function on the value. If the value was modified, it
// is encoded and stored back into the storage.
func (a *typeMutation[T]) Do(
	ctx context.Context,
	key string,
	fn func(T) error,
) (*actionResult[T], error) {
	get := replication.OpGet{
		Level:   a.level,
		Cluster: a.cluster,
		Timeout: a.timeout,
		Logger:  a.logger,
	}

	getResult, err := get.Do(ctx, key)
	if err != nil {
		return nil, err
	}

	value := a.New()

	for _, b := range getResult.Values {
		right := a.New()

		if err := right.FromBytes(b); err != nil {
			return nil, err
		}

		value.Merge(right)
	}

	err = fn(value)
	if err != nil {
		return nil, err
	}

	if value.Modified() {
		put := replication.OpPut{
			Level:   a.level,
			Cluster: a.cluster,
			Timeout: a.timeout,
			Logger:  a.logger,
		}

		valueBytes, err := value.ToBytes()
		if err != nil {
			return nil, err
		}

		putResult, err := put.Do(ctx, key, valueBytes, getResult.Version)
		if err != nil {
			return nil, err
		}

		return &actionResult[T]{
			value:        value,
			version:      putResult.Version,
			acknowledged: putResult.Acknowledged,
			conflict:     len(getResult.Values) > 1,
		}, nil
	}

	return &actionResult[T]{
		version: getResult.Version,
		value:   value,
	}, nil
}

// DoWithConflictRetry runs the mutation function on the value stored at the given key.
// It is similar to Do, but it retries the operation in case of a conflicting write.
func (a *typeMutation[T]) DoWithConflictRetry(
	ctx context.Context,
	key string,
	fn func(T) error,
) (*actionResult[T], error) {
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		res, err := a.Do(ctx, key, fn)

		if err != nil {
			if grpcutil.ErrorCode(err) == codes.AlreadyExists {
				lastErr = err
				continue
			}

			return nil, err
		}

		return res, nil
	}

	return nil, lastErr
}
