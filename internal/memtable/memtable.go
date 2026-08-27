package memtable

import (
	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/skiplist"
)

type MemTable struct {
	data *skiplist.SkipList
}

type ImmutableMemTable struct {
	MemTable *MemTable
	WalID    uint64
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

// GetAt returns the newest version of key visible at maxSeq.
func (m *MemTable) GetAt(key string, maxSeq uint64) (record.Record, bool) {
	return m.data.GetAt(key, maxSeq)
}

func (m *MemTable) Put(rec record.Record) {
	m.data.Put(rec)
}

func (m *MemTable) Len() int {
	return m.data.Len()
}

func (m *MemTable) Records() []record.Record {
	var records []record.Record

	it := m.Iterator()
	defer it.Close()

	for it.Next() {
		records = append(records, it.Record())
	}

	return records
}
