package compaction

import (
	"container/heap"

	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
)

func MergeSSTables(sstables []*sstable.SSTable, newPath string) (*sstable.SSTable, error) {
	iters := make([]*sstable.Iterator, len(sstables))

	h := &MinHeap{}
	heap.Init(h)

	for i, sst := range sstables {
		it, err := sst.Iterator()
		if err != nil {
			closeIterators(iters)
			return nil, err
		}

		iters[i] = it

		if it.Next() {
			heap.Push(h, &HeapItem{
				Record:       it.Record(),
				SSTableIndex: i,
			})
		}

		if err := it.Err(); err != nil {
			closeIterators(iters)
			return nil, err
		}
	}
	defer closeIterators(iters)

	var output []record.Record
	var lastKey string
	firstKey := true

	for h.Len() > 0 {
		// pop min pair from heap, write it to new SSTable
		item := heap.Pop(h).(*HeapItem)

		// If duplicate, skip
		if firstKey || item.Record.Key != lastKey {
			if !item.Record.Deleted {
				output = append(output, item.Record)
			}

			lastKey = item.Record.Key
			firstKey = false
		}

		it := iters[item.SSTableIndex]
		if it.Next() {
			heap.Push(h, &HeapItem{
				Record:       it.Record(),
				SSTableIndex: item.SSTableIndex,
			})
		}

		if err := it.Err(); err != nil {
			return nil, err
		}
	}

	return sstable.CreateFromRecords(output, newPath, sstable.DefaultBlockSize)
}

func closeIterators(iters []*sstable.Iterator) {
	for _, it := range iters {
		if it != nil {
			it.Close()
		}
	}
}
