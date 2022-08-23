package skiplist

import "golang.org/x/exp/constraints"

func OrderedComparator[T constraints.Ordered](a, b T) int {
	if a > b {
		return 1
	} else if a < b {
		return -1
	} else {
		return 0
	}
}

var (
	IntComparator    = OrderedComparator[int]
	StringComparator = OrderedComparator[string]
)
