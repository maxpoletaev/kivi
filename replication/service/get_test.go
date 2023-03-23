package service

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kivi/internal/vclock"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeclient"
)

func TestMergeValues(t *testing.T) {
	tests := map[string]struct {
		values     []nodeValue
		wantResult mergeResult
	}{
		"NoValues": {
			values: nil,
			wantResult: mergeResult{
				Version: vclock.NewEncoded(),
				Values:  nil,
			},
		},
		"SingleValue": {
			values: []nodeValue{
				{
					NodeID: 1,
					VersionedValue: nodeclient.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1}),
					},
				},
			},
			wantResult: mergeResult{
				Values: []nodeValue{
					{
						NodeID: 1,
						VersionedValue: nodeclient.VersionedValue{
							Version: vclock.NewEncoded(vclock.V{1: 1}),
						},
					},
				},
				Version:       vclock.NewEncoded(vclock.V{1: 1}),
				StaleReplicas: nil,
			},
		},
		"StaleAndDuplicates": {
			values: []nodeValue{
				{
					NodeID: 1,
					VersionedValue: nodeclient.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1, 2: 1}),
						Data:    []byte("older value"),
					},
				},
				{
					NodeID: 2,
					VersionedValue: nodeclient.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1, 2: 2}),
						Data:    []byte("newer value"),
					},
				},
				{
					NodeID: 3,
					VersionedValue: nodeclient.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1, 2: 1}),
						Data:    []byte("older value"),
					},
				},
				{
					NodeID: 4,
					VersionedValue: nodeclient.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 2, 2: 1}),
						Data:    []byte("newer concurrent value"),
					},
				},
				{
					NodeID: 5,
					VersionedValue: nodeclient.VersionedValue{
						Version: vclock.NewEncoded(vclock.V{1: 1, 2: 2}),
						Data:    []byte("newer value duplicate"),
					},
				},
			},
			wantResult: mergeResult{
				Values: []nodeValue{
					{
						NodeID: 2,
						VersionedValue: nodeclient.VersionedValue{
							Version: vclock.NewEncoded(vclock.V{1: 1, 2: 2}),
							Data:    []byte("newer value"),
						},
					},
					{
						NodeID: 4,
						VersionedValue: nodeclient.VersionedValue{
							Version: vclock.NewEncoded(vclock.V{1: 2, 2: 1}),
							Data:    []byte("newer concurrent value"),
						},
					},
				},
				Version:       vclock.NewEncoded(vclock.V{1: 2, 2: 2}),
				StaleReplicas: []membership.NodeID{1, 3},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := mergeVersions(tt.values)
			require.NoError(t, err)
			require.Equal(t, len(tt.wantResult.Values), len(result.Values))
			require.Equal(t, tt.wantResult.Version, result.Version)
			require.ElementsMatch(t, tt.wantResult.StaleReplicas, result.StaleReplicas)
			require.ElementsMatch(t, tt.wantResult.Values, result.Values)
		})
	}
}
