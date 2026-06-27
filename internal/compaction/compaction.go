package compaction

import (
	"container/heap"
	"encoding/gob"
	"io"
	"os"

	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
)

func MergeSSTables(sstables []*sstable.SSTable, newPath string) (*sstable.SSTable, error) {
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

	// push records onto heap

	// intialize the min-heap
	h := &MinHeap{}
	heap.Init(h)

	for i, decoder := range decoders {
		var rec record.Record
		err := decoder.Decode(&rec)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return nil, err
		}

		heap.Push(h, &HeapItem{
			Record:       rec,
			SSTableIndex: i,
		})
	}

	var lastKey string
	firstKey := true

	for h.Len() > 0 {
		// pop min pair from heap, write it to new SSTable
		item := heap.Pop(h).(*HeapItem)

		// If duplicate, skip
		if firstKey || item.Record.Key != lastKey {
			if !item.Record.Deleted {
				if err := encoder.Encode(item.Record); err != nil {
					return nil, err
				}
			}

			lastKey = item.Record.Key
			firstKey = false
		}

		// push next pair from same SSTable into heap
		var next record.Record
		err := decoders[item.SSTableIndex].Decode(&next)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return nil, err
		}

		heap.Push(h, &HeapItem{
			Record:       next,
			SSTableIndex: item.SSTableIndex,
		})
	}

	return &sstable.SSTable{Path: newPath}, nil
}
