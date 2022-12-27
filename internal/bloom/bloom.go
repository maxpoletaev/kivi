package bloom

import (
	"hash"
	"math"

	"github.com/twmb/murmur3"
)

// Filter is an implementation of a Bloom filter. It uses k hash functions
// to hash the data and set the corresponding bits to 1. The bits are stored
// in a byte slice. The size of the byte slice is m/8, where m is the number
// of bits in the filter.
type Filter struct {
	value   []byte
	hashers []hash.Hash32
}

// New returns a new Bloom filter. The size is defined by the length of the
// value, which is a byte slice that stores the bits. Therefore the size is
// always a multiple of 8. K defines the number of hash functions used to
// hash the data.
func New(value []byte, k int) *Filter {
	if len(value) == 0 {
		panic("bloom: value must not be empty")
	}

	hashers := make([]hash.Hash32, k)
	for i := 0; i < k; i++ {
		hashers[i] = murmur3.SeedNew32(uint32(i))
	}

	return &Filter{
		value:   value,
		hashers: hashers,
	}
}

// NewWithProbability returns a new Bloom filter with the given number of
// expected elements and the probability of a false positive. The size of
// the filter is calculated using the formula from the paper "Less Hashing,
// Same Performance: Building a Better Bloom Filter" by Adam Kirsch and
// Michael Mitzenmacher.
func NewWithProbability(n int, p float64) (*Filter, int, int) {
	m := int(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
	k := int(float64(m) / float64(n) * math.Log(2))
	return New(make([]byte, m/8), k), m, k
}

// Add adds the data to the Bloom filter. The data is hashed k times and
// the corresponding bits are set to 1. It modifies the original byte slice.
func (bf *Filter) Add(data []byte) {
	var idx uint32

	m := len(bf.value) * 8

	for _, h := range bf.hashers {
		h.Write(data)
		defer h.Reset()

		idx = h.Sum32() % uint32(m)
		bf.value[idx/8] |= 1 << (idx % 8)
	}
}

// Check checks if the data is in the Bloom filter. The data is hashed
// k times and the corresponding bits are checked. If any of the bits
// is 0, the data is not in the Bloom filter. If all the bits are 1,
// the data may be in the Bloom filter, but there is a chance of a
// false positive.
func (bf *Filter) Check(data []byte) bool {
	var idx uint32

	m := len(bf.value) * 8

	for _, h := range bf.hashers {
		h.Write(data)
		defer h.Reset()

		idx = h.Sum32() % uint32(m)
		if bf.value[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}

	return true
}

// Value returns a copy of the byte slice that stores the bits.
func (bf *Filter) Bytes() []byte {
	value := make([]byte, len(bf.value))
	copy(value, bf.value)
	return value
}
