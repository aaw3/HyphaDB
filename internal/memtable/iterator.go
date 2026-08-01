package memtable

import (
	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/skiplist"
)

type Iterator struct {
	it      *skiplist.Iterator
	current record.Record
}

// Compile-time check that *Iterator satisfies the shared record iterator API.
var _ record.Iterator = (*Iterator)(nil)

func (m *MemTable) Iterator() *Iterator {
	return &Iterator{
		it: m.data.Iterator(),
	}
}

func (it *Iterator) Next() bool {
	if !it.it.Valid() {
		return false
	}

	it.current = it.it.Record()
	it.it.Next()
	return true
}

func (it *Iterator) Record() record.Record {
	return it.current
}

func (it *Iterator) Err() error {
	return nil
}

func (it *Iterator) Close() error {
	return nil
}
