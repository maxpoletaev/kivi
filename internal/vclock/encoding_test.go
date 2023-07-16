package vclock

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	tests := map[string]struct {
		vector     Version
		wantString string
	}{
		"EmptyVector": {
			vector:     Empty(),
			wantString: "{}",
		},
		"ZeroValue": {
			vector:     Version{1: 0},
			wantString: "{}",
		},
		"SingleValue": {
			vector:     Version{1: 10},
			wantString: "{1=A}",
		},
		"RollOver": {
			vector:     Version{1: math.MaxInt32, 2: math.MinInt32},
			wantString: "{1=ZIK0ZJ,2=!0}",
		},
		"MultipleValues": {
			vector:     Version{1: 10, 3: 20, 2: 5, 0: 1, 4: -1},
			wantString: "{0=1,1=A,2=5,3=K,4=!ZIK0ZJ}",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.wantString, Encode(tt.vector))
		})
	}
}

func TestDecode(t *testing.T) {
	tests := map[string]struct {
		s          string
		wantVector Version
		wantErr    bool
	}{
		"EmptyString": {
			s:          "",
			wantVector: Version{},
		},
		"EmptyVector": {
			s:          "{}",
			wantVector: Version{},
		},
		"SingleValue": {
			s:          "{1=A}",
			wantVector: Version{1: 10},
		},
		"RollOver": {
			s:          "{1=ZIK0ZJ,2=!0}",
			wantVector: Version{1: math.MaxInt32, 2: math.MinInt32},
		},
		"MultipleValues": {
			s:          "{0=1,1=A,2=5,3=K,4=!ZIK0ZJ}",
			wantVector: Version{0: 1, 1: 10, 2: 5, 3: 20, 4: -1},
		},
		"InvalidString": {
			s:       "invalid",
			wantErr: true,
		},
		"InvalidVersion": {
			s:       "{1=invalid}",
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			v, err := Decode(tt.s)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantVector, v)
			}
		})
	}
}

func TestEncodeDecode(t *testing.T) {
	tests := map[string]struct {
		v Version
	}{
		"EmptyVector": {
			v: Empty(),
		},
		"SingleValue": {
			v: Version{1: 10},
		},
		"MultipleValues": {
			v: Version{1: 10, 3: 20, 2: 5, 0: 1},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			encoded := Encode(tt.v)
			decoded, err := Decode(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.v, decoded, "want: %s, got: %s", tt.v, decoded)
		})
	}
}

func TestEncodeConsistency(t *testing.T) {
	v := make(Version, 100)
	for i := 0; i < 100; i++ {
		v[uint32(i)] = int32(i)
	}

	want := Encode(v)

	// Make hundreds encodings and check that they are all the same.
	for i := 0; i < 100; i++ {
		got := Encode(v)
		if want != got {
			t.Fatalf("encoding not consistent, want: %s, got: %s", want, got)
		}
	}
}
