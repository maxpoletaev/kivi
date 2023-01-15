package vclock

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
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
			vector:     New(V{1: 10, 3: 20, 2: 5, 0: 1}),
			wantString: "{0=1, 1=10, 2=5, 3=20}",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := tt.vector.String()
			assert.Equal(t, tt.wantString, s)
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
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCompare_WithRollover(t *testing.T) {
	a := New(V{1: math.MaxUint32 - 1})
	b := New(V{1: math.MaxUint32})
	assert.Equal(t, Compare(a, b), Before)

	a.Update(1)
	assert.Equal(t, Compare(a, b), Equal)

	a.Update(1) // Overflows, but should still be greater than b.
	assert.Equal(t, uint32(0), a.clocks[1])
	assert.Equal(t, Compare(a, b), After)
}

func TestMerge(t *testing.T) {
	got := Merge(
		New(V{1: 10, 2: 5}),
		New(V{1: 5, 2: 10, 3: 100}),
	)

	want := New(V{1: 10, 2: 10, 3: 100})
	assert.True(t, IsEqual(got, want), "got: %s, want: %s", got, want)
}

func TestMerge_WithRollover(t *testing.T) {
	a := New(V{1: math.MaxUint32, 2: 1})
	b := New(V{1: math.MaxUint32, 2: 2})

	a.Update(1)
	a.Update(1)

	got := Merge(a, b)
	want := New(V{1: 1, 2: 2})
	assert.True(t, IsEqual(got, want), "got: %s, want: %s", got, want)

	// Ensure that the newly produced vector correctly keeps the info aboutrollovers.
	// The value in the vector c is larger but do not have the rollover flag set, so
	// in the merge we shold still get {1=1, 2=2}.
	c := New(V{1: math.MaxUint32})
	got = Merge(got, c)
	want = New(V{1: 1, 2: 2})
	assert.True(t, IsEqual(got, want), "got: %s, want: %s", got, want)
}
