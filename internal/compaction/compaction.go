package compaction

import (
	"container/heap"

	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
)

func MergeSSTables(sstables []*sstable.SSTable, newPath string) (*sstable.SSTable, error) {
	return MergeSSTablesWithRetention(sstables, newPath, nil)
}

// MergeSSTablesWithRetention merges tables while preserving the versions
// visible to the oldest active reader. A nil oldestReader keeps only the
// newest non-tombstone version.
func MergeSSTablesWithRetention(
	sstables []*sstable.SSTable,
	newPath string,
	oldestReader *uint64,
) (*sstable.SSTable, error) {
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
	retainedVisible := false

	for h.Len() > 0 {
		item := heap.Pop(h).(*HeapItem)

		if firstKey || item.Record.Key != lastKey {
			lastKey = item.Record.Key
			firstKey = false
			retainedVisible = false
		}

		keep := false
		if oldestReader == nil {
			// Without active readers, only the newest version is needed.
			keep = !retainedVisible && !item.Record.Deleted
			retainedVisible = true
		} else if item.Record.Seq > *oldestReader {
			// Newer versions may be visible to newer readers or future reads.
			keep = true
		} else if !retainedVisible {
			// Keep the newest version visible to the oldest reader, including
			// a tombstone.
			keep = true
			retainedVisible = true
		}

		if keep {
			output = append(output, item.Record)
		}

		// Once a version at or below the retention boundary has been kept,
		// all remaining versions for this key are older and can be skipped.
		if oldestReader != nil && item.Record.Seq <= *oldestReader {
			retainedVisible = true
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
