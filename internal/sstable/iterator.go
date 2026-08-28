package sstable

import (
	"os"
	"sort"

	"github.com/aaw3/hyphadb/internal/record"
)

type Iterator struct {
	sst          *SSTable
	file         *os.File
	index        []IndexEntry
	blockIndex   int
	blockRecords []record.Record
	recordIndex  int
	current      record.Record
	err          error
	refHeld      bool
	closed       bool
}

// Compile-time check that *Iterator satisfies the shared record iterator API.
var _ record.Iterator = (*Iterator)(nil)
var _ record.SeekableIterator = (*Iterator)(nil)

func (s *SSTable) Iterator() (*Iterator, error) {
	if err := s.Acquire(); err != nil {
		return nil, err
	}

	if err := s.loadMetadata(); err != nil {
		s.Release()
		return nil, err
	}

	file, err := os.Open(s.Path)
	if err != nil {
		s.Release()
		return nil, err
	}

	s.metaMu.RLock()
	index := s.index
	s.metaMu.RUnlock()

	return &Iterator{
		sst:         s,
		file:        file,
		index:       index,
		blockIndex:  -1,
		recordIndex: -1,
		refHeld:     true,
	}, nil
}

func (it *Iterator) Seek(key string) error {
	if it.err != nil {
		return it.err
	}

	it.blockRecords = nil
	it.recordIndex = -1

	if len(it.index) == 0 {
		it.blockIndex = 0
		return nil
	}

	blockIndex := sort.Search(len(it.index), func(i int) bool {
		return it.index[i].FirstKey > key
	}) - 1
	if blockIndex < 0 {
		blockIndex = 0
	}

	if err := it.loadBlock(blockIndex); err != nil {
		return err
	}

	recordIndex := sort.Search(len(it.blockRecords), func(i int) bool {
		return it.blockRecords[i].Key >= key
	})
	if recordIndex < len(it.blockRecords) {
		it.recordIndex = recordIndex - 1
		return nil
	}

	it.recordIndex = len(it.blockRecords) - 1
	return nil
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}

	it.recordIndex++

	// return  a record while there are still records in the current block
	if it.recordIndex < len(it.blockRecords) {
		it.current = it.blockRecords[it.recordIndex]
		return true
	}

	// move to the next block
	it.blockIndex++
	if it.blockIndex >= len(it.index) {
		return false
	}

	if err := it.loadBlock(it.blockIndex); err != nil {
		it.err = err
		return false
	}

	it.recordIndex = 0

	// if the block has no records, move to the next block
	if len(it.blockRecords) == 0 {
		return it.Next()
	}

	it.current = it.blockRecords[it.recordIndex]
	return true
}

func (it *Iterator) loadBlock(blockIndex int) error {
	logical, err := it.sst.readLogicalBlockFrom(it.file, it.index[blockIndex])
	if err != nil {
		it.err = err
		return err
	}

	records, err := decodeLogicalBlock(logical)
	if err != nil {
		it.err = err
		return err
	}

	it.blockIndex = blockIndex
	it.blockRecords = records
	return nil
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
	var err error
	if it.file != nil {
		err = it.file.Close()
		it.file = nil
	}
	if it.refHeld {
		it.sst.Release()
		it.refHeld = false
	}
	return err
}
