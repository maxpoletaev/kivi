package engine

import (
	"github.com/maxpoletaev/kiwi/internal/lockmap"
	"github.com/maxpoletaev/kiwi/storage"
	"github.com/maxpoletaev/kiwi/storage/lsmtree"
	"github.com/maxpoletaev/kiwi/storage/lsmtree/proto"
)

type Engine struct {
	locks *lockmap.Map[string]
	lsm   *lsmtree.LSMTree
}

func New(lsm *lsmtree.LSMTree) *Engine {
	return &Engine{
		locks: lockmap.New[string](),
		lsm:   lsm,
	}
}

func (s *Engine) Get(key string) ([]storage.Value, error) {
	entry, found, err := s.lsm.Get(key)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, storage.ErrNotFound
	}

	return fromProtoValues(entry.Values), nil
}

func (s *Engine) Put(key string, value storage.Value) error {
	s.locks.Lock(key)
	defer s.locks.Unlock(key)

	var values []storage.Value

	entry, found, err := s.lsm.Get(key)
	if err != nil {
		return err
	} else if found {
		values = fromProtoValues(entry.Values)
	}

	values, err = storage.AppendVersion(values, value)
	if err != nil {
		return err
	}

	entry = &proto.DataEntry{
		Key:    key,
		Values: toProtoValues(values),
	}

	if err := s.lsm.Put(entry); err != nil {
		return err
	}

	return nil
}
