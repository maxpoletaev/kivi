package lockmap

import "sync"

type Map[K comparable] struct {
	mut   sync.Mutex
	locks map[K]*sync.Mutex
}

func New[K comparable]() *Map[K] {
	return &Map[K]{
		locks: make(map[K]*sync.Mutex),
	}
}

func (lm *Map[K]) Lock(key K) {
	lm.mut.Lock()
	defer lm.mut.Unlock()

	if _, ok := lm.locks[key]; !ok {
		lm.locks[key] = &sync.Mutex{}
	}

	lm.locks[key].Lock()
}

func (lm *Map[K]) Unlock(key K) {
	lm.mut.Lock()
	defer lm.mut.Unlock()

	lm.locks[key].Unlock()
	delete(lm.locks, key)
}
