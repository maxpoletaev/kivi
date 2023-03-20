package generic

import "sync"

type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (m *SyncMap[K, V]) Delete(key K) {
	m.m.Delete(key)
}

func (m *SyncMap[K, V]) Load(key K) (value V, ok bool) {
	if v, ok := m.m.Load(key); ok {
		return v.(V), true
	}

	var zero V

	return zero, false
}

func (m *SyncMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	if v, loaded := m.m.LoadOrStore(key, value); loaded {
		return v.(V), true
	}

	return value, false
}

func (m *SyncMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	if v, loaded := m.m.LoadAndDelete(key); loaded {
		return v.(V), true
	}

	var zero V

	return zero, false
}

func (m *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	m.m.Range(func(key, value interface{}) bool {
		return f(key.(K), value.(V))
	})
}

func (m *SyncMap[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}
