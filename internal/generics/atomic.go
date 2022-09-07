package generics

import (
	"sync"
	"sync/atomic"
)

// AtomicValue is the same as atomic.Value with additional type safety.
type AtomicValue[T any] struct {
	sync.Mutex
	value atomic.Value
}

func (v *AtomicValue[T]) Load() T {
	return v.value.Load().(T)
}

func (v *AtomicValue[T]) Store(value T) {
	v.value.Store(value)
}
