package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/membership"
	storagepb "github.com/maxpoletaev/kv/storage/proto"
)

func TestMergeValues(t *testing.T) {
	tests := map[string]struct {
		values     []nodeValue
		wantResult mergeResult
	}{
		"NoValues": {
			values: nil,
			wantResult: mergeResult{
				Values:  nil,
				Version: vclock.Vector{},
			},
		},
		"SingleValue": {
			values: []nodeValue{
				{
					NodeID: 1,
					VersionedValue: &storagepb.VersionedValue{
						Version: vclock.Vector{1: 1},
					},
				},
			},
			wantResult: mergeResult{
				Values: []nodeValue{
					{
						NodeID: 1,
						VersionedValue: &storagepb.VersionedValue{
							Version: vclock.Vector{1: 1},
						},
					},
				},
				Version:       vclock.Vector{1: 1},
				StaleReplicas: nil,
			},
		},
		"StaleAndDuplicates": {
			values: []nodeValue{
				{
					NodeID: 1,
					VersionedValue: &storagepb.VersionedValue{
						Version: vclock.Vector{1: 1, 2: 1},
						Data:    []byte("older value"),
					},
				},
				{
					NodeID: 2,
					VersionedValue: &storagepb.VersionedValue{
						Version: vclock.Vector{1: 1, 2: 2},
						Data:    []byte("newer value"),
					},
				},
				{
					NodeID: 3,
					VersionedValue: &storagepb.VersionedValue{
						Version: vclock.Vector{1: 1, 2: 1},
						Data:    []byte("older value"),
					},
				},
				{
					NodeID: 4,
					VersionedValue: &storagepb.VersionedValue{
						Version: vclock.Vector{1: 2, 2: 1},
						Data:    []byte("newer concurrent value"),
					},
				},
				{
					NodeID: 5,
					VersionedValue: &storagepb.VersionedValue{
						Version: vclock.Vector{1: 1, 2: 2},
						Data:    []byte("newer value duplicate"),
					},
				},
			},
			wantResult: mergeResult{
				Values: []nodeValue{
					{
						NodeID: 2,
						VersionedValue: &storagepb.VersionedValue{
							Version: vclock.Vector{1: 1, 2: 2},
							Data:    []byte("newer value"),
						},
					},
					{
						NodeID: 4,
						VersionedValue: &storagepb.VersionedValue{
							Version: vclock.Vector{1: 2, 2: 1},
							Data:    []byte("newer concurrent value"),
						},
					},
				},
				Version:       vclock.Vector{1: 2, 2: 2},
				StaleReplicas: []membership.NodeID{1, 3},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result := mergeValues(tt.values)
			assert.Equal(t, tt.wantResult, result)
		})
	}
}
