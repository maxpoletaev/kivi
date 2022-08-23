package vclock

import (
	"fmt"
	"strings"

	"github.com/maxpoletaev/kv/internal/generics"
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

func (vc Vector) IncrementFor(id uint32) {
	vc[id]++
}

func (vc Vector) Sub(other Vector) Vector {
	keys := generics.MapKeys(vc, other)

	newvec := make(Vector, len(keys))
	for _, key := range keys {
		newvec[key] = vc[key] - other[key]
	}

	return newvec
}

func (v Vector) Clone() Vector {
	newvec := make(Vector, len(v))
	generics.MapCopy(v, newvec)
	return newvec
}

func (v Vector) String() string {
	b := strings.Builder{}

	b.WriteString("{")

	startLen := b.Len()

	for id, val := range v {
		if b.Len() > startLen {
			b.WriteString(",")
		}

		b.WriteString(fmt.Sprint(id))
		b.WriteString("=")
		b.WriteString(fmt.Sprint(val))
	}

	b.WriteString("}")

	return b.String()
}

func Compare(a, b Vector) Causality {
	var greater, less bool

	for _, key := range generics.MapKeys(a, b) {
		// TODO: handle overflow
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
	keys := generics.MapKeys(a, b)

	clock := make(Vector, len(keys))
	for _, key := range keys {
		clock[key] = generics.Max(a[key], b[key])
	}

	return clock
}
