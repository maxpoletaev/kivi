package generic

import (
	"sort"

	"golang.org/x/exp/constraints"
)

func SortSlice[T constraints.Ordered](arr []T, reverse bool) {
	sort.Slice(arr, func(i, j int) bool {
		return (arr[i] < arr[j]) != reverse
	})
}
