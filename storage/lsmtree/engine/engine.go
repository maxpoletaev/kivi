package engine

import (
	"github.com/maxpoletaev/kivi/internal/lockmap"
	"github.com/maxpoletaev/kivi/storage"
	"github.com/maxpoletaev/kivi/storage/lsmtree"
	"github.com/maxpoletaev/kivi/storage/lsmtree/proto"
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
	} else if !found {
		return nil, storage.ErrNotFound
	}

	return fromProtoValueList(entry.Values), nil
}

func (s *Engine) Put(key string, value storage.Value) error {
	s.locks.Lock(key)
	defer s.locks.Unlock(key)

	var values []storage.Value

	entry, found, err := s.lsm.Get(key)
	if err != nil {
		return err
	} else if found {
		values = fromProtoValueList(entry.Values)
	}

	values, err = storage.AppendVersion(values, value)
	if err != nil {
		return err
	}

	entry = &proto.DataEntry{
		Key:    key,
		Values: toProtoValueList(values),
	}

	if err := s.lsm.Put(entry); err != nil {
		return err
	}

	return nil
}
