package rolling

import (
	"sync"

	"github.com/maxpoletaev/kv/internal/generic"
)

// Counter is a thread-safe counter that can be incremented infinite number of
// times due to keeping track of the rollover events (when the counter value
// overflows).
type Counter[T generic.Number] struct {
	mut      sync.Mutex
	rollover bool
	value    T
}

// NewCounter creates a new rolling counter.
func NewCounter[T generic.Number]() *Counter[T] {
	return &Counter[T]{}
}

// Inc increments the counter and returns the new value and a boolean flag
// indicating whether the current state of the rollover.
func (c *Counter[T]) Inc() (T, bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	old := c.value

	c.value++

	if old > c.value {
		c.rollover = !c.rollover
	}

	return c.value, c.rollover
}

// Value returns the current value of the counter.
func (c *Counter[T]) Value() T {
	c.mut.Lock()
	defer c.mut.Unlock()

	return c.value
}

// Rollover returns the current state of the rollover.
func (c *Counter[T]) Rollover() bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	return c.rollover
}

// Less returns true if the first counter is less than the second one.
func Less[T generic.Number](a, b *Counter[T]) bool {
	if a.Rollover() == b.Rollover() {
		return a.Value() < b.Value()
	}

	return a.Value() > b.Value()
}

// Equal returns true if the first counter is equal to the second one.
func Equal[T generic.Number](a, b *Counter[T]) bool {
	return a.Rollover() == b.Rollover() && a.Value() == b.Value()
}
