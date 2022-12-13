package inmemory

import (
	"errors"
	"sync"

	"github.com/maxpoletaev/kv/storage"
	"github.com/maxpoletaev/kv/storage/skiplist"
)

type inMemory struct {
	mu   sync.RWMutex
	data *skiplist.Skiplist[string, []storage.StoredValue]
}

func New() *inMemory {
	return newWithData(skiplist.New[string, []storage.StoredValue](skiplist.StringComparator))
}

func newWithData(data *skiplist.Skiplist[string, []storage.StoredValue]) *inMemory {
	return &inMemory{data: data}
}

func (s *inMemory) Get(key string) ([]storage.StoredValue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values, err := s.data.Get(key)
	if err != nil {
		if errors.Is(err, skiplist.ErrNotFound) {
			return nil, storage.ErrNotFound
		}

		return nil, err
	}

	return values, nil
}

func (s *inMemory) Put(key string, value storage.StoredValue) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	values, err := s.data.Get(key)
	if err != nil {
		if errors.Is(err, skiplist.ErrNotFound) {
			values = nil
		} else {
			return err
		}
	}

	values, err = storage.AppendVersion(values, value)
	if err != nil {
		return err
	}

	s.data.Insert(key, values)

	return nil
}
