package vclock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeDecode(t *testing.T) {
	tests := map[string]struct {
		v *Vector
	}{
		"EmptyVector": {
			v: New(),
		},
		"SingleValue": {
			v: New(V{1: 10}),
		},
		"MultipleValues": {
			v: New(V{1: 10, 3: 20, 2: 5, 0: 1}),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := Encode(tt.v)
			assert.NoError(t, err)

			got, err := Decode(s)
			assert.NoError(t, err)

			assert.Equal(t, tt.v, got, "want: %s, got: %s", tt.v, got)
		})
	}
}

func TestEncodeConsistency(t *testing.T) {
	m := make(V, 100)
	for i := 0; i < 100; i++ {
		m[uint32(i)] = uint32(i)
	}

	v := New(m)
	want := MustEncode(v)

	// Make hundred encodings and check that they are all the same.
	for i := 0; i < 100; i++ {
		got := MustEncode(v)
		if want != got {
			t.Fatalf("encoding not consistent, want: %s, got: %s", want, got)
		}
	}
}
