package skiplist

import "golang.org/x/exp/constraints"

var (
	IntComparator     = orderedComparator[int]
	Int8Comparator    = orderedComparator[int8]
	Int16Comparator   = orderedComparator[int16]
	Int32Comparator   = orderedComparator[int32]
	Int64Comparator   = orderedComparator[int64]
	UintComparator    = orderedComparator[uint]
	Uint8Comparator   = orderedComparator[uint8]
	Uint16Comparator  = orderedComparator[uint16]
	Uint32Comparator  = orderedComparator[uint32]
	Uint64Comparator  = orderedComparator[uint64]
	Float32Comparator = orderedComparator[float32]
	Float64Comparator = orderedComparator[float64]
	StringComparator  = orderedComparator[string]
)

func InverseComparator[T constraints.Ordered](comparator Comparator[T]) Comparator[T] {
	return func(a, b T) int { return -comparator(a, b) }
}

func orderedComparator[T constraints.Ordered](a, b T) int {
	if a > b {
		return 1
	} else if a < b {
		return -1
	} else {
		return 0
	}
}
