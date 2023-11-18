package replication

import (
	"fmt"

	"golang.org/x/exp/maps"

	"github.com/maxpoletaev/kivi/internal/vclock"
	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/noderpc"
)

type nodeValue struct {
	NodeID membership.NodeID
	noderpc.VersionedValue
}

type mergeResult struct {
	version    string
	values     []nodeValue
	staleNodes []membership.NodeID
}

func mergeVersions(values []nodeValue) (mergeResult, error) {
	valueVersion := make([]vclock.Version, len(values))

	// Keep decoded version for each ret.
	for i, v := range values {
		version, err := vclock.FromString(v.Version)
		if err != nil {
			return mergeResult{}, fmt.Errorf("invalid version: %w", err)
		}

		valueVersion[i] = version
	}

	// Merge all versions into one.
	mergedVersion := vclock.Empty()
	for i := 0; i < len(values); i++ {
		mergedVersion = vclock.Merge(mergedVersion, valueVersion[i])
	}

	if len(values) < 2 {
		return mergeResult{
			version: vclock.ToString(mergedVersion),
			values:  values,
		}, nil
	}

	var staleNodes []membership.NodeID

	uniqueValues := make(map[string]nodeValue)

	// Identify the highest version among all values.
	highest := valueVersion[0]
	for i := 1; i < len(values); i++ {
		if vclock.Compare(highest, valueVersion[i]) == vclock.Before {
			highest = valueVersion[i]
		}
	}

	for i := 0; i < len(values); i++ {
		value := values[i]

		// Ignore the values that clearly precede the highest version.
		// Keep track of the replicas that returned outdated values.
		if vclock.Compare(valueVersion[i], highest) == vclock.Before {
			staleNodes = append(staleNodes, value.NodeID)
			continue
		}

		// Keep unique values only, based on the version.
		if _, ok := uniqueValues[value.Version]; !ok {
			uniqueValues[value.Version] = value
		}
	}

	return mergeResult{
		version:    vclock.ToString(mergedVersion),
		values:     maps.Values(uniqueValues),
		staleNodes: staleNodes,
	}, nil
}
