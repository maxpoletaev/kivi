package bloom

import (
	"fmt"
	"hash"
	"hash/fnv"
)

type fnv32 struct {
	hash.Hash32
	seed uint32
}

func newFnv32(seed uint32) hash.Hash32 {
	h := fnv.New32()

	_, err := h.Write([]byte{
		byte(seed),
		byte(seed >> 8),
		byte(seed >> 16),
		byte(seed >> 24),
	})

	if err != nil {
		panic(fmt.Sprintf("bloom: fnv32: %v", err))
	}

	return &fnv32{
		seed:   seed,
		Hash32: h,
	}
}

func (f *fnv32) Sum32() uint32 {
	return f.Hash32.Sum32()
}

func (f *fnv32) Reset() {
	f.Hash32.Reset()

	_, err := f.Write([]byte{
		byte(f.seed),
		byte(f.seed >> 8),
		byte(f.seed >> 16),
		byte(f.seed >> 24),
	})

	if err != nil {
		panic(fmt.Sprintf("bloom: fnv32: %v", err))
	}
}
