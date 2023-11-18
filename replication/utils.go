package replication

import (
	"context"

	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/noderpc"
)

func countAlive(members []membership.Node) (alive int) {
	for i := range members {
		if members[i].IsReachable() {
			alive++
		}
	}

	return
}

func putValue(ctx context.Context, conn noderpc.Client, key string, value []byte, version string, primary bool) (string, error) {
	resp, err := conn.StoragePut(ctx, key, noderpc.VersionedValue{
		Version: version,
		Data:    value,
	}, primary)

	if err != nil {
		return "", err
	}

	return resp.Version, nil
}

func putTombstone(ctx context.Context, conn noderpc.Client, key, version string, primary bool) (string, error) {
	resp, err := conn.StoragePut(ctx, key, noderpc.VersionedValue{
		Version:   version,
		Tombstone: true,
	}, primary)

	if err != nil {
		return "", err
	}

	return resp.Version, nil
}
