package memtable

import (
	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/skiplist"
)

type MemTable struct {
	data *skiplist.SkipList
}

func New() *MemTable {
	return &MemTable{
		data: skiplist.New(),
	}
}

// Operations are delegated to the underlying skiplist

func (m *MemTable) Get(key string) (record.Record, bool) {
	rec, ok := m.data.Get(key)
	if !ok {
		return record.Record{}, false
	}
	return rec, true
}

func (m *MemTable) Put(rec record.Record) {
	m.data.Put(rec)
}

func (m *MemTable) Records() []record.Record {
	return m.data.Records()
}
