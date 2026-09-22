package sstable

import (
	"os"

	"github.com/aaw3/hyphadb/internal/record"
)

type Iterator struct {
	sst         *SSTable
	file        *os.File
	index       []IndexEntry
	blockIndex  int
	blockCursor logicalBlockCursor
	current     record.Record
	pending     record.Record
	hasPending  bool
	err         error
	refHeld     bool
	closed      bool
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
		sst:        s,
		file:       file,
		index:      index,
		blockIndex: -1,
		refHeld:    true,
	}, nil
}

func (it *Iterator) Seek(key string) error {
	if it.err != nil {
		return it.err
	}

	it.blockCursor = logicalBlockCursor{}
	it.current = record.Record{}
	it.pending = record.Record{}
	it.hasPending = false

	if len(it.index) == 0 {
		it.blockIndex = 0
		return nil
	}

	blockIndex := firstPossibleBlock(it.index, key)

	if err := it.loadBlock(blockIndex); err != nil {
		return err
	}

	for {
		rec, ok := it.blockCursor.next()
		if !ok {
			if it.blockCursor.err != nil {
				it.err = it.blockCursor.err
				return it.err
			}
			return nil
		}
		if rec.Key >= key {
			it.pending = rec
			it.hasPending = true
			return nil
		}
	}
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}

	if it.hasPending {
		it.current = it.pending
		it.pending = record.Record{}
		it.hasPending = false
		return true
	}

	for {
		rec, ok := it.blockCursor.next()
		if ok {
			it.current = rec
			return true
		}
		if it.blockCursor.err != nil {
			it.err = it.blockCursor.err
			return false
		}

		it.blockIndex++
		if it.blockIndex >= len(it.index) {
			return false
		}

		if err := it.loadBlock(it.blockIndex); err != nil {
			return false
		}
	}
}

func (it *Iterator) loadBlock(blockIndex int) error {
	logical, err := it.sst.readLogicalBlockFrom(it.file, it.index[blockIndex])
	if err != nil {
		it.err = err
		return err
	}

	cursor, err := newLogicalBlockCursor(logical)
	if err != nil {
		it.err = err
		return err
	}

	it.blockIndex = blockIndex
	it.blockCursor = cursor
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
