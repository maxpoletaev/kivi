package lsmtree

import (
	"testing"

	"github.com/stretchr/testify/require"
	protobuf "google.golang.org/protobuf/proto"

	"github.com/maxpoletaev/kiwi/storage/lsmtree/proto"
)

func tableFromEntries(t *testing.T, dir string, id int64, entries []*proto.DataEntry) *SSTable {
	t.Helper()

	memt, err := createMemtable(dir)
	if err != nil {
		t.Fatal(err)
	}

	defer memt.Discard()

	for _, entry := range entries {
		if err := memt.Put(entry); err != nil {
			t.Fatal(err)
		}
	}

	sst, err := flushToDisk(memt, flushOpts{
		bloomProb: 0.01,
		prefix:    dir,
		tableID:   id,
	})
	if err != nil {
		t.Fatal(err)
	}

	return sst
}

func TestMergeTables(t *testing.T) {
	dir := t.TempDir()

	table1 := tableFromEntries(t, dir, 1, []*proto.DataEntry{
		{Key: "a", Values: []*proto.Value{{Data: []byte("a1")}}},
	})

	table2 := tableFromEntries(t, dir, 2, []*proto.DataEntry{
		{Key: "a", Values: []*proto.Value{{Data: []byte("a2")}}},
		{Key: "b", Values: []*proto.Value{{Data: []byte("b2")}}},
	})

	table3 := tableFromEntries(t, dir, 3, []*proto.DataEntry{
		{Key: "b", Values: []*proto.Value{{Data: []byte("b3")}}},
		{Key: "c", Values: []*proto.Value{{Data: []byte("c3")}}},
	})

	sst, err := mergeTables([]*SSTable{table1, table2, table3}, flushOpts{
		bloomProb: 0.01,
		prefix:    dir,
		tableID:   4,
	})

	require.NoError(t, err)

	require.Equal(t, int64(3), sst.NumEntries)

	wantEntries := []*proto.DataEntry{
		{Key: "a", Values: []*proto.Value{{Data: []byte("a2")}}},
		{Key: "b", Values: []*proto.Value{{Data: []byte("b3")}}},
		{Key: "c", Values: []*proto.Value{{Data: []byte("c3")}}},
	}

	for _, want := range wantEntries {
		got, found, err := sst.Get(want.Key)

		require.NoError(t, err)
		require.True(t, found, "key not found: %s", want.Key)
		require.True(t, protobuf.Equal(got, want), "wrong value for key %s: %s", want.Key, got.Values[0].Data)
	}
}
