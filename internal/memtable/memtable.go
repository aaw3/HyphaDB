package memtable

import "github.com/aaw3/hyphadb/internal/record"

type MemTable struct {
	// Rely on Go's primitive map until we have a functional base system
	data map[string]record.Entry
}

func New() *MemTable {
	return &MemTable{
		data: make(map[string]record.Entry),
	}
}

func (m *MemTable) Get(key string) (record.Entry, bool) {
	value, exists := m.data[key]
	if !exists {
		return record.Entry{}, false
	}
	return value, true
}

func (m *MemTable) Put(key string, value record.Entry) {
	m.data[key] = value
}

func (m *MemTable) Delete(key string) {
	m.data[key] = record.Entry{
		Deleted: true,
	}
}

func (m *MemTable) Entries() map[string]record.Entry {
	entries := make(map[string]record.Entry, len(m.data))
	for k, v := range m.data {
		entries[k] = v
	}
	return entries
}
