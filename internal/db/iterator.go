package db

import (
	"container/heap"

	"github.com/aaw3/hyphadb/internal/record"
)

type IteratorOptions struct {
	Start string
	End   string
}

type Iterator struct {
	db      *DB
	opts    IteratorOptions
	sources []record.Iterator
	heap    iteratorHeap
	current record.Record
	err     error
	closed  bool
}

// Compile-time check that *Iterator satisfies the shared record iterator API.
var _ record.Iterator = (*Iterator)(nil)

type iteratorItem struct {
	record      record.Record
	sourceIndex int
}

type iteratorHeap []*iteratorItem

func (h iteratorHeap) Len() int {
	return len(h)
}

func (h iteratorHeap) Less(i, j int) bool {
	return h[i].record.Key < h[j].record.Key
}

func (h iteratorHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *iteratorHeap) Push(x any) {
	*h = append(*h, x.(*iteratorItem))
}

func (h *iteratorHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

func (db *DB) NewIterator(opts IteratorOptions) (*Iterator, error) {
	db.mu.RLock()

	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}

	it := &Iterator{
		db:   db,
		opts: opts,
	}

	it.sources = append(it.sources, db.memtable.Iterator())

	for i := len(db.immutableMemtables) - 1; i >= 0; i-- {
		it.sources = append(it.sources, db.immutableMemtables[i].MemTable.Iterator())
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		sstIt, err := db.sstables[i].Iterator()
		if err != nil {
			it.closeSources()
			db.mu.RUnlock()
			return nil, err
		}

		it.sources = append(it.sources, sstIt)
	}

	heap.Init(&it.heap)

	for i := range it.sources {
		if opts.Start != "" {
			if seeker, ok := it.sources[i].(record.SeekableIterator); ok {
				if err := seeker.Seek(opts.Start); err != nil {
					it.closeSources()
					db.mu.RUnlock()
					return nil, err
				}
			}
		}

		// Prime the merge heap with the first in-range record from each source.
		if err := it.advanceSource(i); err != nil {
			it.closeSources()
			db.mu.RUnlock()
			return nil, err
		}
	}

	return it, nil
}

func (it *Iterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}

	for it.heap.Len() > 0 {
		item := heap.Pop(&it.heap).(*iteratorItem)
		key := item.record.Key
		best := item.record

		if err := it.advanceSource(item.sourceIndex); err != nil {
			it.err = err
			return false
		}

		for it.heap.Len() > 0 && it.heap[0].record.Key == key {
			item = heap.Pop(&it.heap).(*iteratorItem)
			if item.record.Seq > best.Seq {
				best = item.record
			}

			if err := it.advanceSource(item.sourceIndex); err != nil {
				it.err = err
				return false
			}
		}

		if best.Deleted {
			continue
		}

		it.current = best
		return true
	}

	return false
}

func (it *Iterator) Record() record.Record {
	return it.current
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true
	err := it.closeSources()
	it.db.mu.RUnlock()
	return err
}

// advanceSource moves one source iterator forward until it finds the next record
// inside the requested range, then pushes that record into the merge heap. When
// available, NewIterator seeks sources to Start before this function runs
func (it *Iterator) advanceSource(sourceIndex int) error {
	source := it.sources[sourceIndex]

	for source.Next() {
		rec := source.Record()

		if it.opts.Start != "" && rec.Key < it.opts.Start {
			continue
		}

		// Source iterators are sorted, so reaching End means this source is done.
		if it.opts.End != "" && rec.Key >= it.opts.End {
			return nil
		}

		heap.Push(&it.heap, &iteratorItem{
			record:      rec,
			sourceIndex: sourceIndex,
		})
		return nil
	}

	return source.Err()
}

func (it *Iterator) closeSources() error {
	var err error

	for _, source := range it.sources {
		if sourceErr := source.Close(); err == nil && sourceErr != nil {
			err = sourceErr
		}
	}

	return err
}
