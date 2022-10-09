package consistency

import (
	"fmt"
)

// Level defines Cassandra-like consistency levels.
type Level int

const (
	// LevelOne implies that an acknowledgement from a single node is enough to complete the operation.
	LevelOne Level = iota

	// LevelTwo needs acknowledgement from at least two nodes to complete the operation.
	LevelTwo

	// LevelQuorum needs acknowledgement from N/2+1 of nodes to complete the operation.
	LevelQuorum

	// LevelAll requires acknowledgement from ALL nodes.
	LevelAll
)

// N returns how many replicas are needed to satisfy the consistency level if there are n replicas in total.
// Note that the returned number can be greater than total number of nodes. For example the consistency
// level Two always requires two nodes even if total number of nodes is 1.
func (l Level) N(total int) int {
	switch l {
	case LevelOne:
		return 1
	case LevelTwo:
		return 2
	case LevelQuorum:
		return total/2 + 1
	case LevelAll:
		return total
	default:
		panic(fmt.Sprintf("unknown consistency level: %d", l))
	}
}

// String returns string representation of the consistency level.
func (l Level) String() string {
	switch l {
	case LevelOne:
		return "one"
	case LevelTwo:
		return "two"
	case LevelQuorum:
		return "quorum"
	case LevelAll:
		return "all"
	default:
		return ""
	}
}
