package vclock

import (
	"github.com/maxpoletaev/kivi/internal/generic"
)

// Causality is a type that represents the causality relationship between two vectors.
type Causality int

const (
	Before Causality = iota + 1
	Concurrent
	After
	Equal
)

func (c Causality) String() string {
	switch c {
	case Before:
		return "Before"
	case Concurrent:
		return "Concurrent"
	case After:
		return "After"
	case Equal:
		return "Equal"
	default:
		return ""
	}
}

// Version represents a vector clock, which is a mapping of node IDs to clock values.
// The clock value is a monotonically increasing counter that is incremented every time
// the node makes an update. The clock value is used to determine the causality relationship
// between two events. If the clock value of a node is greater than the clock value of
// another node, it means that the first event happened after the second event.
// The implementation uses a 32-bit integer to store the clock value. The leftmost bit
// is used to indicate whether the clock value has rolled over. If the leftmost bit is
// set, the clock value has rolled over.
type Version map[uint32]uint64

// Empty returns a new version vector.
func Empty() Version {
	return make(Version)
}

// String returns a string representation of the vector clock.
// The string representation is a comma-separated list of key=value pairs, where the
// key is the node ID and the value is the clock value: {1=1, 2=2}. If the clock value
// has rolled over, the leftmost bit is set and the value is negative: {1=-1, 2=-2}.
func (vc Version) String() string {
	return Encode(vc)
}

// Copy returns a copy of the vector clock.
func (vc Version) Copy() Version {
	newvec := make(Version, len(vc))
	generic.MapCopy(newvec, vc)
	return newvec
}

// Increment increments the clock value of the given node.
func (vc Version) Increment(nodeID uint32) {
	vc[nodeID]++

	if vc[nodeID] == 0 {
		panic("clock value overflow")
	}
}

// Compare returns the causality relationship between two vectors.
// Compare(a, b) == After means that a happened after b, and so on.
// Comparing values that have rolled over is tricky, so the implementation
// uses the following rules: if the clock value of a node is greater than
// the clock value of another node, and the rollover flags are different,
// it means that the value has wrapped around and we need to invert the
// comparison. For example, if a clock value is 2^32-1 and the other clock
// value is 0, and the rollover flags are different, it means that the clock
// value of the first node has wrapped around and the second node has not.
// In this case, the first node is considered to be less than the second node.
func Compare(a, b Version) Causality {
	var greater, less bool

	for _, key := range generic.MapKeys(a, b) {
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

// IsEqual returns true if the two vectors are equal.
func IsEqual(a, b Version) bool {
	return Compare(a, b) == Equal
}

// Merge returns a new vector that is the result of taking the maximum value for
// each key in the two vectors. The operation is commutative and associative, so
// that Merge(a, Merge(b, c)) == Merge(Merge(a, b), c).
func Merge(a, b Version) Version {
	keys := generic.MapKeys(a, b)
	merged := make(Version, len(keys))

	for _, key := range keys {
		if a[key] > b[key] {
			merged[key] = a[key]
		} else {
			merged[key] = b[key]
		}
	}

	return merged
}
