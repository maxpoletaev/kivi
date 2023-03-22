package rolling

import (
	"github.com/maxpoletaev/kiwi/internal/generic"
)

const (
	Less    = -1
	Equal   = 0
	Greater = 1
)

// Compare compares two signed values taking type rolling into account.
func Compare[T generic.Signed](a, b T) int {
	var (
		absA = generic.Abs(a)
		absB = generic.Abs(b)
	)

	if absA > absB {
		if a < 0 {
			return Less
		} else {
			return Greater
		}
	} else if absA < absB {
		if a < 0 {
			return Greater
		} else {
			return Less
		}
	}

	return Equal
}

// Max returns the maximum of two signed values taking type rolling into account.
func Max[T generic.Signed](a, b T) T {
	if Compare(a, b) == Greater {
		return a
	}

	return b
}
