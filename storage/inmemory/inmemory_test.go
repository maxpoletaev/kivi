package inmemory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kivi/internal/skiplist"
	"github.com/maxpoletaev/kivi/internal/vclock"
	"github.com/maxpoletaev/kivi/storage"
)

func TestGet(t *testing.T) {
	lst := skiplist.New[string, []storage.Value](skiplist.StringComparator)
	lst.Insert("key", []storage.Value{
		{
			Version: vclock.Empty(),
			Data:    []byte("value"),
		},
	})

	memstore := newWithData(lst)
	values, err := memstore.Get("key")

	assert.NoError(t, err)
	assert.Len(t, values, 1)
}

func TestGetFails_NotFound(t *testing.T) {
	lst := skiplist.New[string, []storage.Value](skiplist.StringComparator)

	memstore := newWithData(lst)
	values, err := memstore.Get("non-existing-key")

	assert.ErrorIs(t, err, storage.ErrNotFound)
	assert.Nil(t, values)
}

func TestPut_NewValue(t *testing.T) {
	lst := skiplist.New[string, []storage.Value](skiplist.StringComparator)

	version := vclock.Empty()
	version.Increment(99)

	memstore := newWithData(lst)
	err := memstore.Put("key", storage.Value{
		Data:    []byte("value"),
		Version: version,
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, lst.Size())

	listValues, found := lst.Get("key")
	assert.True(t, found)
	assert.Len(t, listValues, 1)
	assert.Equal(t, []byte("value"), listValues[0].Data)
	assert.Equal(t, int32(1), listValues[0].Version[99])
}

// TestPut_NewVersion verifies the case when there is already a value in the store
// and the new value overtakes the existing one. Only the newest value should be preserved.
func TestPut_NewVersion(t *testing.T) {
	list := skiplist.New[string, []storage.Value](skiplist.StringComparator)
	version := vclock.Empty()
	version.Increment(1)

	list.Insert("key", []storage.Value{{
		Data:    []byte("value"),
		Version: version.Copy(),
	}})

	version.Increment(1)
	version.Increment(2)

	memstore := newWithData(list)
	err := memstore.Put("key", storage.Value{
		Data:    []byte("new value"),
		Version: version.Copy(),
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, list.Size())

	listValue, found := list.Get("key")

	assert.True(t, found)
	assert.Len(t, listValue, 1)
	assert.Equal(t, listValue[0].Data, []byte("new value"))
}

// TestPut_ConflictingValue verifies that put works correcty in case there is already a value
// in the store and the new value conflicts with the existing one according to the version
// vector. Both values should be preserved.
func TestPut_ConflictingVersion(t *testing.T) {
	list := skiplist.New[string, []storage.Value](skiplist.StringComparator)

	version := vclock.Empty()
	version.Increment(1)

	list.Insert("key", []storage.Value{{
		Data:    []byte("value"),
		Version: version.Copy(),
	}})

	conflictingVersion := vclock.Empty()
	conflictingVersion.Increment(2)

	memstore := newWithData(list)
	err := memstore.Put("key", storage.Value{
		Data:    []byte("another value"),
		Version: conflictingVersion.Copy(),
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, list.Size())

	listValue, found := list.Get("key")

	assert.True(t, found)
	assert.Len(t, listValue, 2)
	assert.Equal(t, listValue[0].Data, []byte("value"))
	assert.Equal(t, listValue[1].Data, []byte("another value"))
}

// TestPutFails_ObsoleteVersion verifies the case when there is already a value in the store
// and it is never than the incoming value. The incoming write should be rejected with
// the "ObsoleteWrite" error.
func TestPutFails_ObsoleteVersion(t *testing.T) {
	list := skiplist.New[string, []storage.Value](skiplist.StringComparator)

	version := vclock.Empty()
	version.Increment(1)
	version.Increment(1)

	list.Insert("key", []storage.Value{{
		Data:    []byte("newer value"),
		Version: version.Copy(),
	}})

	olderVersion := vclock.Empty()
	olderVersion.Increment(1)

	memstore := newWithData(list)
	err := memstore.Put("key", storage.Value{
		Data:    []byte("older value"),
		Version: olderVersion.Copy(),
	})

	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrObsolete)
}

// TestPutFails_SameVersion verifies the case when the value with the same version is pushed
// twice to the store. The second write should fail with the "ObsoleteWrite" error.
func TestPutFails_SameVersion(t *testing.T) {
	list := skiplist.New[string, []storage.Value](skiplist.StringComparator)

	version := vclock.Empty()
	version.Increment(1)

	list.Insert("key", []storage.Value{{
		Data:    []byte("value"),
		Version: version.Copy(),
	}})

	memstore := newWithData(list)
	err := memstore.Put("key", storage.Value{
		Data:    []byte("another value"),
		Version: version.Copy(),
	})

	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrObsolete)
}
