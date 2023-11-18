package service

import (
	"context"
	"time"

	kitlog "github.com/go-kit/log"

	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/replication"
	"github.com/maxpoletaev/kivi/replication/consistency"
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

type dataTypeAction[T dataType[T]] struct {
	New func() T

	cluster membership.Cluster
	level   consistency.Level
	logger  kitlog.Logger
	timeout time.Duration
}

func (a *dataTypeAction[T]) do(
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
