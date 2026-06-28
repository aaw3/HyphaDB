package memtable

import "github.com/aaw3/hyphadb/internal/record"

type MemTable struct {
	// Rely on Go's primitive map until we have a functional base system
	data map[string]record.Record
}

func New() *MemTable {
	return &MemTable{
		data: make(map[string]record.Record),
	}
}

func (m *MemTable) Get(key string) (record.Record, bool) {
	rec, exists := m.data[key]
	if !exists {
		return record.Record{}, false
	}
	return rec, true
}

func (m *MemTable) Put(rec record.Record) {
	m.data[rec.Key] = rec
}

func (m *MemTable) Records() map[string]record.Record {
	records := make(map[string]record.Record, len(m.data))
	for k, v := range m.data {
		records[k] = v
	}
	return records
}
