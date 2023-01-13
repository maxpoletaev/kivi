package lsmtree

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpoletaev/kiwi/internal/protoio"
	"github.com/maxpoletaev/kiwi/storage/lsmtree/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateMemtable(t *testing.T) {
	tempDir := t.TempDir()

	memt, err := createMemtable(tempDir)
	require.NoError(t, err)
	defer memt.CloseAndDiscard()

	require.Equal(t, 0, memt.Len())
	require.FileExists(t, filepath.Join(tempDir, memt.WALFile))
}

func TestOpenMemtable(t *testing.T) {
	tempDir := t.TempDir()

	walFile, err := os.OpenFile(
		filepath.Join(tempDir, "1.wal"), os.O_CREATE|os.O_WRONLY, 0o644)
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

	memt, err := openMemtable(&MemtableInfo{
		ID:      1,
		WALFile: "1.wal",
	}, tempDir)
	require.NoError(t, err)
	defer memt.CloseAndDiscard()

	got, ok := memt.Get("key")
	require.True(t, ok)

	require.Equal(t, 1, memt.Len())
	require.Equal(t, "key", got.Key)
	require.Equal(t, 1, len(got.Values))
	require.Equal(t, []byte("value"), got.Values[0].Data)
}

func TestMemtable_GetAfterPut(t *testing.T) {
	tempDir := t.TempDir()

	memt, err := createMemtable(tempDir)
	require.NoError(t, err)
	defer memt.CloseAndDiscard()

	memt.Put(&proto.DataEntry{
		Key: "key",
		Values: []*proto.Value{
			{
				Version: "",
				Data:    []byte("value"),
			},
		},
	})

	require.Equal(t, 1, memt.Len())

	got, ok := memt.Get("key")
	require.True(t, ok)
	require.Equal(t, "key", got.Key)
	require.Equal(t, 1, len(got.Values))
	require.Equal(t, []byte("value"), got.Values[0].Data)
}

func TestMemtable_PutRecordedInWAL(t *testing.T) {
	tempDir := t.TempDir()

	memt, err := createMemtable(tempDir)
	require.NoError(t, err)
	defer memt.CloseAndDiscard()

	memt.Put(&proto.DataEntry{
		Key: "key",
		Values: []*proto.Value{
			{
				Version: "",
				Data:    []byte("value"),
			},
		},
	})

	file, err := os.OpenFile(
		filepath.Join(tempDir, memt.WALFile), os.O_RDONLY, 0o644)
	require.NoError(t, err)
	defer file.Close()

	walReader := protoio.NewReader(file)
	entry := &proto.DataEntry{}

	n, err := walReader.ReadNext(entry)
	require.NoError(t, err)
	assert.True(t, n > 0)

	require.Equal(t, 1, memt.Len())
	require.Equal(t, "key", entry.Key)
	require.Equal(t, 1, len(entry.Values))
	require.Equal(t, []byte("value"), entry.Values[0].Data)
}
