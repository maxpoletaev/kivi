package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kivi/internal/vclock"
)

func TestAddVersion(t *testing.T) {
	type test struct {
		currentValues []Value
		incomingValue Value
		wantResult    []Value
		wantErr       error
	}

	tests := map[string]test{
		"NoCurrentValues": {
			currentValues: nil,
			incomingValue: Value{
				Data:    []byte("value"),
				Version: vclock.Version{1: 1},
			},
			wantResult: []Value{
				{
					Data:    []byte("value"),
					Version: vclock.Version{1: 1},
				},
			},
		},
		"IncomingValueOvertakesOneCurrentValue": {
			currentValues: []Value{
				{
					Data:    []byte("current value"),
					Version: vclock.Version{1: 1},
				},
			},
			incomingValue: Value{
				Data:    []byte("new value"),
				Version: vclock.Version{1: 2},
			},
			wantResult: []Value{
				{
					Data:    []byte("new value"),
					Version: vclock.Version{1: 2},
				},
			},
		},
		"IncomingValueOvertakesOneOfCurrentValues": {
			currentValues: []Value{
				{
					Data:    []byte("old value 1"),
					Version: vclock.Version{1: 1},
				},
				{
					Data:    []byte("old value 2"),
					Version: vclock.Version{2: 1},
				},
			},
			incomingValue: Value{
				Data:    []byte("new value 1"),
				Version: vclock.Version{1: 2},
			},
			wantResult: []Value{
				{
					Data:    []byte("old value 2"),
					Version: vclock.Version{2: 1},
				},
				{
					Data:    []byte("new value 1"),
					Version: vclock.Version{1: 2},
				},
			},
		},
		"IncomingValueOvertakesAllOfCurrentValues": {
			currentValues: []Value{
				{
					Data:    []byte("old value 1"),
					Version: vclock.Version{1: 1},
				},
				{
					Data:    []byte("old value 2"),
					Version: vclock.Version{2: 1},
				},
			},
			incomingValue: Value{
				Data:    []byte("new value"),
				Version: vclock.Version{1: 2, 2: 1},
			},
			wantResult: []Value{
				{
					Data:    []byte("new value"),
					Version: vclock.Version{1: 2, 2: 1},
				},
			},
		},
		"IncomingValueIsOlderThanTheCurrentValue": {
			currentValues: []Value{
				{
					Data:    []byte("never value"),
					Version: vclock.Version{1: 2},
				},
			},
			incomingValue: Value{
				Data:    []byte("older value"),
				Version: vclock.Version{1: 1},
			},
			wantErr: ErrObsolete,
		},
		"IncomingValueIsTheSameAsCurrentValue": {
			currentValues: []Value{
				{
					Data:    []byte("value"),
					Version: vclock.Version{1: 1},
				},
			},
			incomingValue: Value{
				Data:    []byte("value"),
				Version: vclock.Version{1: 1},
			},
			wantErr: ErrObsolete,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := AppendVersion(tt.currentValues, tt.incomingValue)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.wantResult, result)
		})
	}
}

func TestRemoveVersion(t *testing.T) {

}
