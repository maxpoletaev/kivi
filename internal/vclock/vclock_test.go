package vclock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	tests := map[string]struct {
		a        Vector
		b        Vector
		expected Causality
	}{
		"Before": {
			a:        Vector{0: 1, 1: 1, 2: 1},
			b:        Vector{0: 2, 1: 1, 2: 1},
			expected: Before,
		},
		"After": {
			a:        Vector{0: 3, 1: 2, 2: 1},
			b:        Vector{0: 2, 1: 1, 2: 1},
			expected: After,
		},
		"Equal": {
			a:        Vector{0: 1, 1: 1},
			b:        Vector{0: 1, 1: 1, 2: 0},
			expected: Equal,
		},
		"Concurrent": {
			a:        Vector{0: 1, 1: 0},
			b:        Vector{0: 0, 1: 1},
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

func TestMerge(t *testing.T) {
	result := Merge(
		Vector{1: 10, 2: 5},
		Vector{1: 5, 2: 10, 3: 100},
	)

	expected := Vector{
		1: 10, 2: 10, 3: 100,
	}

	assert.Equal(t, expected, result)
}
