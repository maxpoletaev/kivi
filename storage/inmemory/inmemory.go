package inmemory

import (
	"github.com/maxpoletaev/kiwi/internal/lockmap"
	"github.com/maxpoletaev/kiwi/internal/skiplist"
	"github.com/maxpoletaev/kiwi/storage"
)

type Engine struct {
	data  *skiplist.Skiplist[string, []storage.Value]
	locks *lockmap.Map[string]
}

func New() *Engine {
	return newWithData(skiplist.New[string, []storage.Value](skiplist.StringComparator))
}

func newWithData(data *skiplist.Skiplist[string, []storage.Value]) *Engine {
	return &Engine{
		locks: lockmap.New[string](),
		data:  data,
	}
}

func (s *Engine) Get(key string) ([]storage.Value, error) {
	values, found := s.data.Get(key)
	if !found {
		return nil, storage.ErrNotFound
	}

	return values, nil
}

func (s *Engine) Put(key string, value storage.Value) error {
	// Since we read the value before updating it, we need to lock the key to avoid
	// loosing versions during concurrent updates of the same key. The skiplist
	// itself is thread-safe, that is why we do not lock it in Conn.
	s.locks.Lock(key)
	defer s.locks.Unlock(key)

	values, _ := s.data.Get(key)

	values, err := storage.AppendVersion(values, value)
	if err != nil {
		return err
	}

	s.data.Insert(key, values)

	return nil
}

func (s *Engine) Scan(key string) storage.ScanIterator {
	it := s.data.ScanFrom(key)
	return &Iterator{it: it}
}
