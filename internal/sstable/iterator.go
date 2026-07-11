package sstable

import (
	"os"

	"github.com/aaw3/hyphadb/internal/record"
)

type Iterator struct {
	sst          *SSTable
	file         *os.File
	blockIndex   int
	blockRecords []record.Record
	recordIndex  int
	current      record.Record
	err          error
}

func (s *SSTable) Iterator() (*Iterator, error) {
	if err := s.loadIndex(); err != nil {
		return nil, err
	}

	file, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}

	return &Iterator{
		sst:         s,
		file:        file,
		blockIndex:  -1,
		recordIndex: -1,
	}, nil
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
	if it.blockIndex >= len(it.sst.index) {
		return false
	}

	// read the next block from the SSTable file
	physical, err := readBlockFrom(it.file, it.sst.index[it.blockIndex])
	if err != nil {
		it.err = err
		return false
	}

	// decode the physical block into records
	records, err := decodeBlock(physical)
	if err != nil {
		it.err = err
		return false
	}

	// reset the record index and set the current block records
	it.blockRecords = records
	it.recordIndex = 0

	// if the block has no records, move to the next block
	if len(it.blockRecords) == 0 {
		return it.Next()
	}

	it.current = it.blockRecords[it.recordIndex]
	return true
}

func (it *Iterator) Record() record.Record {
	return it.current
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() error {
	if it.file == nil {
		return nil
	}
	return it.file.Close()
}
