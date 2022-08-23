package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddVersion(t *testing.T) {
	type test struct {
		currentValues []StoredValue
		incomingValue StoredValue
		wantResult    []StoredValue
		wantErr       error
	}

	tests := map[string]test{
		"NoCurrentValues": {
			currentValues: nil,
			incomingValue: StoredValue{
				Blob:    []byte("value"),
				Version: map[uint32]uint64{1: 1},
			},
			wantResult: []StoredValue{
				{
					Blob:    []byte("value"),
					Version: map[uint32]uint64{1: 1},
				},
			},
		},
		"IncomingValueOvertakesOneCurrentValue": {
			currentValues: []StoredValue{
				{
					Blob:    []byte("current value"),
					Version: map[uint32]uint64{1: 1},
				},
			},
			incomingValue: StoredValue{
				Blob:    []byte("new value"),
				Version: map[uint32]uint64{1: 2},
			},
			wantResult: []StoredValue{
				{
					Blob:    []byte("new value"),
					Version: map[uint32]uint64{1: 2},
				},
			},
		},
		"IncomingValueOvertakesOneOfCurrentValues": {
			currentValues: []StoredValue{
				{
					Blob:    []byte("old value 1"),
					Version: map[uint32]uint64{1: 1},
				},
				{
					Blob:    []byte("old value 2"),
					Version: map[uint32]uint64{2: 1},
				},
			},
			incomingValue: StoredValue{
				Blob:    []byte("new value 1"),
				Version: map[uint32]uint64{1: 2},
			},
			wantResult: []StoredValue{
				{
					Blob:    []byte("old value 2"),
					Version: map[uint32]uint64{2: 1},
				},
				{
					Blob:    []byte("new value 1"),
					Version: map[uint32]uint64{1: 2},
				},
			},
		},
		"IncomingValueOvertakesAllOfCurrentValues": {
			currentValues: []StoredValue{
				{
					Blob:    []byte("old value 1"),
					Version: map[uint32]uint64{1: 1},
				},
				{
					Blob:    []byte("old value 2"),
					Version: map[uint32]uint64{2: 1},
				},
			},
			incomingValue: StoredValue{
				Blob:    []byte("new value"),
				Version: map[uint32]uint64{1: 2, 2: 1},
			},
			wantResult: []StoredValue{
				{
					Blob:    []byte("new value"),
					Version: map[uint32]uint64{1: 2, 2: 1},
				},
			},
		},
		"IncomingValueIsOlderThanTheCurrentValue": {
			currentValues: []StoredValue{
				{
					Blob:    []byte("never value"),
					Version: map[uint32]uint64{1: 2},
				},
			},
			incomingValue: StoredValue{
				Blob:    []byte("older value"),
				Version: map[uint32]uint64{1: 1},
			},
			wantErr: ErrObsoleteWrite,
		},
		"IncomingValueIsTheSameAsCurrentValue": {
			currentValues: []StoredValue{
				{
					Blob:    []byte("value"),
					Version: map[uint32]uint64{1: 1},
				},
			},
			incomingValue: StoredValue{
				Blob:    []byte("value"),
				Version: map[uint32]uint64{1: 1},
			},
			wantErr: ErrObsoleteWrite,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := AddVersion(tt.currentValues, tt.incomingValue)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantResult, result)
		})
	}
}