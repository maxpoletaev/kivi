package lsmtree

import (
	"fmt"
	"os"
	"testing"

	"github.com/maxpoletaev/kv/internal/protoio"
	"github.com/maxpoletaev/kv/storage/lsmtree/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateMemtable(t *testing.T) {
	dir := t.TempDir()
	walFilename := fmt.Sprintf("%s/1.wal", dir)

	mt, err := CreateMemtable(walFilename)
	require.NoError(t, err)

	require.Equal(t, 0, mt.Len())
}

func TestMemtable_GetAfterRestore(t *testing.T) {
	dir := t.TempDir()
	walPath := fmt.Sprintf("%s/1.wal", dir)

	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer walFile.Close()

	writer := protoio.NewWriter(walFile)
	writer.Append(&proto.DataEntry{
		Key: "key",
		Values: []*proto.Value{
			{
				Version: "",
				Data:    []byte("value"),
			},
		},
	})

	mt, err := RestoreMemtable(walPath)
	require.NoError(t, err)

	got, ok := mt.Get("key")
	require.True(t, ok)

	require.Equal(t, 1, mt.Len())
	require.Equal(t, "key", got.Key)
	require.Equal(t, 1, len(got.Values))
	require.Equal(t, []byte("value"), got.Values[0].Data)
}

func TestMemtable_Put(t *testing.T) {
	dir := t.TempDir()

	walFilename := fmt.Sprintf("%s/1.wal", dir)
	mt, err := CreateMemtable(walFilename)
	require.NoError(t, err)

	mt.Put(&proto.DataEntry{
		Key: "key",
		Values: []*proto.Value{
			{
				Version: "",
				Data:    []byte("value"),
			},
		},
	})

	file, err := os.OpenFile(walFilename, os.O_RDONLY, 0o644)
	require.NoError(t, err)
	defer file.Close()

	walReader := protoio.NewReader(file)
	entry := &proto.DataEntry{}

	n, err := walReader.ReadNext(entry)
	require.NoError(t, err)
	assert.True(t, n > 0)

	require.Equal(t, 1, mt.Len())
	require.Equal(t, "key", entry.Key)
	require.Equal(t, 1, len(entry.Values))
	require.Equal(t, []byte("value"), entry.Values[0].Data)
}
