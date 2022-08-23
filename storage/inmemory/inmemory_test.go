package inmemory

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxpoletaev/kv/internal/vclock"
	"github.com/maxpoletaev/kv/storage"
	"github.com/maxpoletaev/kv/storage/skiplist"
)

func TestGet(t *testing.T) {
	lst := skiplist.New[string, []storage.StoredValue](skiplist.StringComparator)
	lst.Insert("key", []storage.StoredValue{{Version: make(vclock.Vector), Blob: []byte("value")}})

	memstore := newWithData(lst)
	values, err := memstore.Get("key")

	assert.NoError(t, err)
	assert.Len(t, values, 1)
}

func TestGetFails_NotFound(t *testing.T) {
	lst := skiplist.New[string, []storage.StoredValue](skiplist.StringComparator)

	memstore := newWithData(lst)
	values, err := memstore.Get("non-existing-key")

	assert.ErrorIs(t, err, storage.ErrNotFound)
	assert.Nil(t, values)
}

func TestPut_NewValue(t *testing.T) {
	lst := skiplist.New[string, []storage.StoredValue](skiplist.StringComparator)

	version := vclock.New()
	version.IncrementFor(99)

	memstore := newWithData(lst)
	err := memstore.Put("key", storage.StoredValue{
		Blob:    []byte("value"),
		Version: version,
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, lst.Size())

	listValues, err := lst.Get("key")
	assert.NoError(t, err)
	assert.Len(t, listValues, 1)
	assert.Equal(t, []byte("value"), listValues[0].Blob)
	assert.Equal(t, uint64(1), listValues[0].Version[99])
}

// TestPut_NewVersion verifies the case when there is already a value in the store
// and the new value overtakes the existing one. Only the newest value should be preserved.
func TestPut_NewVersion(t *testing.T) {
	list := skiplist.New[string, []storage.StoredValue](skiplist.StringComparator)

	version := make(vclock.Vector)
	version.IncrementFor(1)

	list.Insert("key", []storage.StoredValue{{
		Blob:    []byte("value"),
		Version: version.Clone(),
	}})

	version.IncrementFor(1)
	version.IncrementFor(2)

	memstore := newWithData(list)
	err := memstore.Put("key", storage.StoredValue{
		Blob:    []byte("new value"),
		Version: version.Clone(),
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, list.Size())

	listValue, err := list.Get("key")

	assert.NoError(t, err)
	assert.Len(t, listValue, 1)
	assert.Equal(t, listValue[0].Blob, []byte("new value"))
}

// TestPut_ConflictingValue verifies that put works correcty in case there is already a value
// in the store and the new value conflicts with the existing one according to the version
// vector. Both values should be preserved.
func TestPut_ConflictingVersion(t *testing.T) {
	list := skiplist.New[string, []storage.StoredValue](skiplist.StringComparator)

	version := make(vclock.Vector)
	version.IncrementFor(1)

	list.Insert("key", []storage.StoredValue{{
		Blob:    []byte("value"),
		Version: version.Clone(),
	}})

	conflictingVersion := make(vclock.Vector)
	conflictingVersion.IncrementFor(2)

	memstore := newWithData(list)
	err := memstore.Put("key", storage.StoredValue{
		Blob:    []byte("another value"),
		Version: conflictingVersion.Clone(),
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, list.Size())

	listValue, err := list.Get("key")

	assert.NoError(t, err)
	assert.Len(t, listValue, 2)
	assert.Equal(t, listValue[0].Blob, []byte("value"))
	assert.Equal(t, listValue[1].Blob, []byte("another value"))
}

// TestPutFails_ObsoleteVersion verifies the case when there is already a value in the store
// and it is never than the incoming value. The incoming write should be rejected with
// the "ObsoleteWrite" error.
func TestPutFails_ObsoleteVersion(t *testing.T) {
	list := skiplist.New[string, []storage.StoredValue](skiplist.StringComparator)

	version := make(vclock.Vector)
	version.IncrementFor(1)
	version.IncrementFor(1)

	list.Insert("key", []storage.StoredValue{{
		Blob:    []byte("newer value"),
		Version: version.Clone(),
	}})

	olderVersion := make(vclock.Vector)
	olderVersion.IncrementFor(1)

	memstore := newWithData(list)
	err := memstore.Put("key", storage.StoredValue{
		Blob:    []byte("older value"),
		Version: olderVersion.Clone(),
	})

	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrObsoleteWrite)
}

// TestPutFails_SameVersion verifies the case when the value with the same version is pushed
// twice to the store. The second write should fail with the "ObsoleteWrite" error.
func TestPutFails_SameVersion(t *testing.T) {
	list := skiplist.New[string, []storage.StoredValue](skiplist.StringComparator)

	version := make(vclock.Vector)
	version.IncrementFor(1)

	list.Insert("key", []storage.StoredValue{{
		Blob:    []byte("value"),
		Version: version.Clone(),
	}})

	memstore := newWithData(list)
	err := memstore.Put("key", storage.StoredValue{
		Blob:    []byte("another value"),
		Version: version.Clone(),
	})

	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrObsoleteWrite)
}
