package memtable

// generic types, should use string for key and []byte for value in the future
type MemTable[K comparable, V any] struct {
	// Rely on Go's primitive map until we have a functional base system
	data map[K]V
}

func New[K comparable, V any]() *MemTable[K, V] {
	return &MemTable[K, V]{
		data: make(map[K]V),
	}
}

func (m *MemTable[K, V]) Get(key K) (V, bool) {
	value, exists := m.data[key]
	var zero V
	if !exists {
		return zero, false
	}
	return value, true
}

func (m *MemTable[K, V]) Put(key K, value V) {
	m.data[key] = value
}

func (m *MemTable[K, V]) Entries() map[K]V {
	entries := make(map[K]V, len(m.data))
	for k, v := range m.data {
		entries[k] = v
	}
	return entries
}
