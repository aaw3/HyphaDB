package memtable

import (
	"sort"

	"github.com/aaw3/hyphadb/internal/record"
)

// SnapshotIterator iterates over an immutable copy of a memtable's records.
// The caller must prevent writes while SnapshotIterator is being created.
type SnapshotIterator struct {
	records []record.Record
	index   int
	current record.Record
}

var _ record.Iterator = (*SnapshotIterator)(nil)
var _ record.SeekableIterator = (*SnapshotIterator)(nil)

func (m *MemTable) SnapshotIterator() *SnapshotIterator {
	records := m.Records()
	for i := range records {
		records[i].Value = append([]byte(nil), records[i].Value...)
	}

	return &SnapshotIterator{
		records: records,
		index:   -1,
	}
}

func (it *SnapshotIterator) Seek(key string) error {
	it.index = sort.Search(len(it.records), func(i int) bool {
		return it.records[i].Key >= key
	}) - 1
	return nil
}

func (it *SnapshotIterator) Next() bool {
	it.index++
	if it.index >= len(it.records) {
		return false
	}

	it.current = it.records[it.index]
	return true
}

func (it *SnapshotIterator) Record() record.Record {
	return it.current
}

func (it *SnapshotIterator) Err() error {
	return nil
}

func (it *SnapshotIterator) Close() error {
	return nil
}
