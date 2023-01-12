package inmemory

import (
	"github.com/maxpoletaev/kv/internal/lockmap"
	"github.com/maxpoletaev/kv/internal/skiplist"
	"github.com/maxpoletaev/kv/storage"
)

type InMemoryEngine struct {
	data  *skiplist.Skiplist[string, []storage.Value]
	locks *lockmap.Map[string]
}

func New() *InMemoryEngine {
	return newWithData(skiplist.New[string, []storage.Value](skiplist.StringComparator))
}

func newWithData(data *skiplist.Skiplist[string, []storage.Value]) *InMemoryEngine {
	return &InMemoryEngine{
		locks: lockmap.New[string](),
		data:  data,
	}
}

func (s *InMemoryEngine) Get(key string) ([]storage.Value, error) {
	values, found := s.data.Get(key)
	if !found {
		return nil, storage.ErrNotFound
	}

	return values, nil
}

func (s *InMemoryEngine) Put(key string, value storage.Value) error {
	// Since we read the value before updating it, we need to lock the key to avoid
	// loosing versions during concurrent updates of the same key. The skiplist
	// itself is thread-safe, that is why we do not lock it in Get.
	s.locks.Lock(key)

	values, _ := s.data.Get(key)

	values, err := storage.AppendVersion(values, value)
	if err != nil {
		s.locks.Unlock(key)
		return err
	}

	s.data.Insert(key, values)
	s.locks.Unlock(key)

	return nil
}
