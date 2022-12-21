package bloom

import (
	"hash"
	"unsafe"

	"github.com/maxpoletaev/kv/internal/generic"
	"github.com/twmb/murmur3"
)

// Filter is a generic implementation of a Bloom filter. It uses the
// Murmur3 hash function and the type T must be an unsigned integer type.
// The methods of the Filter should not be called concurrently.
type Filter[T generic.Unsigned] struct {
	value    T
	typeSize int
	hashers  []hash.Hash32
}

// New creates a new Bloom filter with K hash functions and the type T.
// The type T must be an unsigned integer type. The size of the Bloom filter
// is defined by the size of the type T in bits.
func New[T generic.Unsigned](value T, k int) *Filter[T] {
	var t T

	size := unsafe.Sizeof(t)

	hashers := make([]hash.Hash32, k)

	for i := 0; i < k; i++ {
		hashers[i] = murmur3.SeedNew32(uint32(i))
	}

	return &Filter[T]{
		value:    value,
		hashers:  hashers,
		typeSize: int(size) * 8,
	}
}

// Add adds the data to the Bloom filter. The data is hashed
// k times and the corresponding bits are set to 1.
func (bf *Filter[T]) Add(data []byte) {
	var idx uint32

	for _, h := range bf.hashers {
		h.Write(data)
		defer h.Reset()

		idx = h.Sum32() % uint32(bf.typeSize)

		bf.value = bf.value | (1 << idx)
	}
}

// Check checks if the data is in the Bloom filter. The data is hashed
// k times and the corresponding bits are checked. If any of the bits
// is 0, the data is not in the Bloom filter. If all the bits are 1,
// the data may be in the Bloom filter, but there is a chance of a
// false positive.
func (bf *Filter[T]) Check(data []byte) bool {
	var idx uint32

	for _, h := range bf.hashers {
		h.Write(data)
		defer h.Reset()

		idx = h.Sum32() % uint32(bf.typeSize)

		if bf.value&(1<<idx) == 0 {
			return false
		}
	}

	return true
}

// Value returns the value of the Bloom filter. The value is the
// underlying type T. This value can be used to send the bloom filter
// over the network or to store it in a file.
func (bf *Filter[T]) Value() T {
	return bf.value
}
