package bloom

import (
	"fmt"
	"hash/fnv"
	"io"
	"math"
)

func writeSeed(h io.Writer, seed uint32) {
	const goldenRatio = 0x9e3779b9
	s := goldenRatio * seed

	seedBytes := []byte{
		byte(s >> 0),
		byte(s >> 8),
		byte(s >> 16),
		byte(s >> 24),
	}

	if _, err := h.Write(seedBytes); err != nil {
		panic(fmt.Sprintf("bloom: %s", err))
	}
}

// Filter is an implementation of a Bloom filter. It uses k hash functions
// to hash the data and set the corresponding bits to 1. The bits are stored
// in a byte slice. The size of the byte slice is m/8, where m is the number
// of bits in the filter.
type Filter struct {
	value  []byte
	hashes int
}

// New returns a new Bloom filter. The size is defined by the length of the
// value, which is a byte slice that stores the bits. Therefore the size is
// always a multiple of 8. K defines the number of hash functions used to
// hash the data.
func New(value []byte, k int) *Filter {
	if len(value) == 0 {
		panic("bloom: value must not be empty")
	}

	return &Filter{
		value:  value,
		hashes: k,
	}
}

// NewWithProbability returns a new Bloom filter with the given number of
// expected elements and the probability of a false positive.
func NewWithProbability(n int, p float64) *Filter {
	if n <= 0 {
		panic("bloom: n must be greater than 0")
	}

	if p <= 0 || p >= 1 {
		panic("bloom: p must be greater than 0 and less than 1")
	}

	m := int(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
	k := int(float64(m) / float64(n) * math.Log(2))

	return New(make([]byte, m/8), k)
}

// Add adds the data to the Bloom filter. The data is hashed k times and
// the corresponding bits are set to 1. It modifies the original byte slice.
func (bf *Filter) Add(data []byte) {
	m := len(bf.value) * 8

	for i := 0; i < bf.hashes; i++ {
		h := fnv.New32a()
		writeSeed(h, uint32(i+1))

		if _, err := h.Write(data); err != nil {
			panic(fmt.Sprintf("bloom: %s", err))
		}

		idx := h.Sum32() % uint32(m)
		bf.value[idx/8] |= 1 << (idx % 8)
	}
}

// MayContain checks if the data is in the Bloom filter. The data is hashed
// k times and the corresponding bits are checked. If any of the bits
// is 0, the data is not in the Bloom filter. If all the bits are 1,
// the data may be in the Bloom filter, but there is a chance of a
// false positive.
func (bf *Filter) MayContain(data []byte) bool {
	m := len(bf.value) * 8

	for i := 0; i < bf.hashes; i++ {
		h := fnv.New32a()
		writeSeed(h, uint32(i+1))

		if _, err := h.Write(data); err != nil {
			panic(fmt.Sprintf("bloom: %s", err))
		}

		idx := h.Sum32() % uint32(m)
		if bf.value[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}

	return true
}

// Bytes returns a copy of the byte slice that stores the bits.
func (bf *Filter) Bytes() []byte {
	value := make([]byte, len(bf.value))
	copy(value, bf.value)

	return value
}

func (bf *Filter) Size() int {
	return len(bf.value)
}

func (bf *Filter) Hashes() int {
	return bf.hashes
}
