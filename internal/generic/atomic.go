package generic

import (
	"sync"
	"sync/atomic"
)

// Atomic is the same as atomic.Value with additional type safety.
type Atomic[T any] struct {
	sync.Mutex
	value atomic.Value
}

func (v *Atomic[T]) Load() T {
	return v.value.Load().(T)
}

func (v *Atomic[T]) Store(value T) {
	v.value.Store(value)
}
