package consistency

import (
	"fmt"
)

// Level defines Cassandra-like consistency levels.
type Level int

const (
	// One implies that an acknowledgement from a single node is enough to complete the operation.
	One Level = iota

	// Two needs acknowledgement from at least two nodes to complete the operation.
	Two

	// Quorum needs acknowledgement from N/2+1 of nodes to complete the operation.
	Quorum

	// All requires acknowledgement from ALL nodes.
	All
)

// N returns how many replicas are needed to satisfy the consistency level if there are n replicas in total.
// Note that the returned number can be greater than total number of nodes. For example the consistency
// level Two always requires two nodes even if total number of nodes is 1.
func (l Level) N(total int) int {
	switch l {
	case One:
		return 1
	case Two:
		return 2
	case Quorum:
		return total/2 + 1
	case All:
		return total
	default:
		panic(fmt.Sprintf("unknown consistency level: %d", l))
	}
}

// String returns string representation of the consistency level.
func (l Level) String() string {
	switch l {
	case One:
		return "one"
	case Two:
		return "two"
	case Quorum:
		return "quorum"
	case All:
		return "all"
	default:
		return ""
	}
}
