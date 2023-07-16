package vclock

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	tests := map[string]struct {
		a        Version
		b        Version
		expected Causality
	}{
		"Before": {
			a:        Version{0: 1, 1: 1, 2: 1},
			b:        Version{0: 2, 1: 1, 2: 1},
			expected: Before,
		},
		"After": {
			a:        Version{0: 3, 1: 2, 2: 1},
			b:        Version{0: 2, 1: 1, 2: 1},
			expected: After,
		},
		"Equal": {
			a:        Version{0: 1, 1: 1},
			b:        Version{0: 1, 1: 1, 2: 0},
			expected: Equal,
		},
		"Concurrent": {
			a:        Version{0: 1, 1: 0},
			b:        Version{0: 0, 1: 1},
			expected: Concurrent,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result := Compare(tt.a, tt.b)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCompare_WithRollover(t *testing.T) {
	a := Version{1: math.MaxInt32 - 1}
	b := Version{1: math.MaxInt32}
	require.Equal(t, Before, Compare(a, b))

	a.Increment(1)
	require.Equal(t, Equal, Compare(a, b))

	a.Increment(1) // Overflows, but should still be greater than b.
	require.Equal(t, int32(math.MinInt32), a[1])
	require.Equal(t, After, Compare(a, b), "a=%d, b=%d", a, b)
}

func TestMerge(t *testing.T) {
	got := Merge(
		Version{1: 10, 2: 5},
		Version{1: 5, 2: 10, 3: 100},
	)

	want := Version{1: 10, 2: 10, 3: 100}
	require.True(t, IsEqual(got, want), "got: %s, want: %s", got, want)
}

func TestMerge_WithRollover(t *testing.T) {
	a := Version{1: math.MaxInt32, 2: 1}
	b := Version{1: math.MaxInt32, 2: 2}

	got := Merge(a, b)
	want := Version{1: int32(math.MinInt32) + 1, 2: 2}
	require.True(t, IsEqual(got, want), "got: %s, want: %s", got, want)
}
