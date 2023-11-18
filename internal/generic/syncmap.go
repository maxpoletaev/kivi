package generic

import "sync"

// SyncMap is a generic wrapper around sync.Map that allows you to specify the
// key and value types to avoid type assertions and casting.
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

// Delete deletes the value for a key.
func (m *SyncMap[K, V]) Delete(key K) {
	m.m.Delete(key)
}

// Store sets the value for a key.
func (m *SyncMap[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}

// Load returns the value stored in the map for a key, or nil if no
// value is present. The ok result indicates whether value was found in the map.
func (m *SyncMap[K, V]) Load(key K) (value V, ok bool) {
	if v, ok := m.m.Load(key); ok {
		return v.(V), true
	}

	var zero V

	return zero, false
}

// LoadOrStore returns the existing value for the key if present. Otherwise, it
// stores and returns the given value. The loaded result is true if the value was
// loaded, false if stored.
func (m *SyncMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	if v, loaded := m.m.LoadOrStore(key, value); loaded {
		return v.(V), true
	}

	return value, false
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
func (m *SyncMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	if v, loaded := m.m.LoadAndDelete(key); loaded {
		return v.(V), true
	}

	var zero V

	return zero, false
}

// Range calls f sequentially for each key and value present in the map. If f
// returns false, range stops the iteration.
func (m *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	m.m.Range(func(key, value interface{}) bool {
		return f(key.(K), value.(V))
	})
}
