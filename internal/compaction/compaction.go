package compaction

import (
	"container/heap"
	"encoding/gob"
	"io"
	"os"

	"github.com/aaw3/hyphadb/internal/sstable"
)

func MergeSSTables[K comparable, V any](sstables []*sstable.SSTable[K, V], newPath string) (*sstable.SSTable[K, V], error) {
	// initialize new SSTable file
	newFile, err := os.Create(newPath)

	if err != nil {
		return nil, err
	}
	defer newFile.Close()

	encoder := gob.NewEncoder(newFile)

	files := make([]*os.File, len(sstables))        // open all SSTable files
	decoders := make([]*gob.Decoder, len(sstables)) // create decoders for each file
	for i, sst := range sstables {
		files[i], err = os.Open(sst.Path)
		if err != nil {
			return nil, err
		}
		defer files[i].Close()

		decoders[i] = gob.NewDecoder(files[i])
	}

	// read first pair from each SSTable
	pairs := make([]sstable.Pair[K, V], len(sstables))
	emptySSTables := make([]bool, len(sstables))
	for i, decoder := range decoders {
		if err := decoder.Decode(&pairs[i]); err != nil {
			if err == io.EOF {
				emptySSTables[i] = true
				continue
			}
			return nil, err
		}
	}

	// push pairs onto heap
	h := &MinHeap[K, V]{}
	for i, pair := range pairs {
		if !emptySSTables[i] {
			heap.Push(h, &HeapItem[K, V]{Pair: pair, SSTableIndex: i})
		}
	}

	// intialize the min-heap
	heap.Init(h)

	var lastKey K
	firstKey := true

	for h.Len() > 0 {
		// pop min pair from heap, write it to new SSTable
		item := heap.Pop(h).(*HeapItem[K, V])

		// If duplicate, skip
		if !firstKey && item.Pair.Key == lastKey {

		} else {
			if any(item.Pair.Value).(string) != sstable.TOMBSTONE {
				if err := encoder.Encode(item.Pair); err != nil {
					return nil, err
				}
			}
		}

		lastKey = item.Pair.Key
		firstKey = false

		// push next pair from same SSTable into heap
		var nextPair sstable.Pair[K, V]
		if err := decoders[item.SSTableIndex].Decode(&nextPair); err != nil {
			heap.Push(h, &HeapItem[K, V]{Pair: nextPair, SSTableIndex: item.SSTableIndex})
		} else if err == io.EOF {
			return nil, err
		}
	}

	return &sstable.SSTable[K, V]{Path: newPath}, nil
}
