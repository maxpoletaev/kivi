package replication

import (
	"context"

	"github.com/maxpoletaev/kivi/membership"
	"github.com/maxpoletaev/kivi/nodeapi"
	storagepb "github.com/maxpoletaev/kivi/storage/proto"
)

func countAlive(members []membership.Node) (alive int) {
	for i := range members {
		if members[i].IsReachable() {
			alive++
		}
	}

	return
}

func putValue(ctx context.Context, conn *nodeapi.Client, key string, value []byte, version string, primary bool) (string, error) {
	resp, err := conn.Storage.Put(ctx, &storagepb.PutRequest{
		Key:     key,
		Primary: primary,
		Value: &storagepb.VersionedValue{
			Version: version,
			Data:    value,
		},
	})

	if err != nil {
		return "", err
	}

	return resp.Version, nil
}

func putTombstone(ctx context.Context, conn *nodeapi.Client, key, version string, primary bool) (string, error) {
	resp, err := conn.Storage.Put(ctx, &storagepb.PutRequest{
		Key:     key,
		Primary: primary,
		Value: &storagepb.VersionedValue{
			Version:   version,
			Tombstone: true,
		},
	})

	if err != nil {
		return "", err
	}

	return resp.Version, nil
}
