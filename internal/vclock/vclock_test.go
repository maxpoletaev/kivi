package vclock

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVector_String(t *testing.T) {
	tests := map[string]struct {
		vector     *Vector
		wantString string
	}{
		"EmptyVector": {
			vector:     New(),
			wantString: "{}",
		},
		"SingleValue": {
			vector:     New(V{1: 10}),
			wantString: "{1=10}",
		},
		"MultipleValues": {
			vector:     New(V{1: 10, 3: 20, 2: 5, 0: 1, 4: -10}),
			wantString: "{0=1,1=10,2=5,3=20,4=!10}",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := tt.vector.String()
			require.Equal(t, tt.wantString, s)
		})
	}
}

func TestCompare(t *testing.T) {
	tests := map[string]struct {
		a        *Vector
		b        *Vector
		expected Causality
	}{
		"Before": {
			a:        New(V{0: 1, 1: 1, 2: 1}),
			b:        New(V{0: 2, 1: 1, 2: 1}),
			expected: Before,
		},
		"After": {
			a:        New(V{0: 3, 1: 2, 2: 1}),
			b:        New(V{0: 2, 1: 1, 2: 1}),
			expected: After,
		},
		"Equal": {
			a:        New(V{0: 1, 1: 1}),
			b:        New(V{0: 1, 1: 1, 2: 0}),
			expected: Equal,
		},
		"Concurrent": {
			a:        New(V{0: 1, 1: 0}),
			b:        New(V{0: 0, 1: 1}),
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
	a := New(V{1: math.MaxInt32 - 1})
	b := New(V{1: math.MaxInt32})
	require.Equal(t, Before, Compare(a, b))

	a.Update(1)
	require.Equal(t, Equal, Compare(a, b))

	a.Update(1) // Overflows, but should still be greater than b.
	require.Equal(t, int32(math.MinInt32), a.clocks[1])
	require.Equal(t, After, Compare(a, b), "a=%d, b=%d", a, b)
}

func TestMerge(t *testing.T) {
	got := Merge(
		New(V{1: 10, 2: 5}),
		New(V{1: 5, 2: 10, 3: 100}),
	)

	want := New(V{1: 10, 2: 10, 3: 100})
	require.True(t, IsEqual(got, want), "got: %s, want: %s", got, want)
}

func TestMerge_WithRollover(t *testing.T) {
	a := New(V{1: math.MaxInt32, 2: 1})
	b := New(V{1: math.MaxInt32, 2: 2})

	got := Merge(a, b)
	want := New(V{1: int32(math.MinInt32) + 1, 2: 2})
	require.True(t, IsEqual(got, want), "got: %s, want: %s", got, want)
}
