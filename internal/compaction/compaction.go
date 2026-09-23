package compaction

import (
	"container/heap"

	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
)

func MergeSSTables(sstables []*sstable.SSTable, newPath string) (*sstable.SSTable, error) {
	return MergeSSTablesWithOptions(sstables, newPath, MergeOptions{})
}

// MergeOptions controls which obsolete versions may be discarded.
type MergeOptions struct {
	OldestReader   *uint64
	DropTombstones bool
	WriteOptions   *sstable.WriteOptions
}

// MergeSSTablesWithRetention merges tables while preserving the versions
// visible to the oldest active reader. Tombstones are preserved because this
// helper does not know whether older values remain outside the merge inputs.
func MergeSSTablesWithRetention(
	sstables []*sstable.SSTable,
	newPath string,
	oldestReader *uint64,
) (*sstable.SSTable, error) {
	return MergeSSTablesWithOptions(sstables, newPath, MergeOptions{
		OldestReader: oldestReader,
	})
}

// MergeSSTablesWithOptions merges tables while retaining every version needed
// by active readers. A tombstone may only be dropped when all older versions
// of its key are known to be included in the merge.
func MergeSSTablesWithOptions(
	sstables []*sstable.SSTable,
	newPath string,
	options MergeOptions,
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
		if options.OldestReader == nil {
			// Without active readers, only the newest version is needed. Keep a
			// tombstone unless the caller has established that no older value
			// can remain outside this merge.
			if !retainedVisible {
				keep = !item.Record.Deleted || !options.DropTombstones
				retainedVisible = true
			}
		} else if item.Record.Seq > *options.OldestReader {
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
		if options.OldestReader != nil && item.Record.Seq <= *options.OldestReader {
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

	writeOptions := sstable.DefaultWriteOptions()
	if options.WriteOptions != nil {
		writeOptions = *options.WriteOptions
	}
	return sstable.CreateFromRecordsWithOptions(output, newPath, writeOptions)
}

func closeIterators(iters []*sstable.Iterator) {
	for _, it := range iters {
		if it != nil {
			it.Close()
		}
	}
}
