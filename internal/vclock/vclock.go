package vclock

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/maxpoletaev/kv/internal/generic"
)

type Causality int

const (
	Before Causality = iota + 1
	Concurrent
	After
	Equal
)

type Vector map[uint32]uint64

func New() Vector {
	return make(Vector)
}

func (vc Vector) Increment(id uint32) {
	vc[id]++
}

func (vc Vector) Sub(other Vector) Vector {
	keys := generic.MapKeys(vc, other)

	newvec := make(Vector, len(keys))
	for _, key := range keys {
		newvec[key] = vc[key] - other[key]
	}

	return newvec
}

func (v Vector) Clone() Vector {
	newvec := make(Vector, len(v))
	generic.MapCopy(v, newvec)
	return newvec
}

func (v Vector) String() string {
	b := strings.Builder{}

	b.WriteString("{")

	keys := generic.MapKeys(v)

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for i, key := range keys {
		if i > 0 {
			b.WriteString(", ")
		}

		b.WriteString(fmt.Sprint(key))
		b.WriteString("=")
		b.WriteString(fmt.Sprint(v[key]))
	}

	b.WriteString("}")

	return b.String()
}

func (v Vector) Hash() uint32 {
	h := fnv.New32()

	_, err := h.Write([]byte(v.String()))
	if err != nil {
		return 0
	}

	return h.Sum32()
}

func Compare(a, b Vector) Causality {
	var greater, less bool

	for _, key := range generic.MapKeys(a, b) {
		// FIXME: handle overflow
		if a[key] > b[key] {
			greater = true
		} else if a[key] < b[key] {
			less = true
		}
	}

	switch {
	case greater && !less:
		return After
	case less && !greater:
		return Before
	case !less && !greater:
		return Equal
	default:
		return Concurrent
	}
}

func Merge(a, b Vector) Vector {
	keys := generic.MapKeys(a, b)

	clock := make(Vector, len(keys))
	for _, key := range keys {
		clock[key] = generic.Max(a[key], b[key])
	}

	return clock
}
