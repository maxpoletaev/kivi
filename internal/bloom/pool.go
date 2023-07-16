package bloom

import "sync"

// Pool is a collection of bloom filters, which can be reused, to
// allow concurrent checks and reduce memory allocations.
type Pool struct {
	pool sync.Pool
}

// NewPool creates a new pool of bloom filters. Each filter will be
// initialized with the given data and k. The data is not copied, so
// the caller must ensure that it is not modified.
func NewPool(data []byte, k int) *Pool {
	return &Pool{
		pool: sync.Pool{
			New: func() any {
				return New(data, k)
			},
		},
	}
}

// Check checks if the given data is in the bloom filter.
func (bp *Pool) Check(data []byte) bool {
	bf := bp.pool.Get().(*Filter)
	defer bp.pool.Put(bf)
	return bf.Check(data)
}
