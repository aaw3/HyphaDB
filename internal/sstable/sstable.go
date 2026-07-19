package sstable

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/aaw3/hyphadb/internal/bloom"
)

var ErrNotFound = errors.New("key not found")
var ErrDeleted = errors.New("key has been deleted")
var ErrUnsortedRecords = errors.New("records are not sorted")

type SSTable struct {
	Path string

	metaMu sync.RWMutex
	index  []IndexEntry
	filter *bloom.Filter
}

func (s *SSTable) Open(key string) ([]byte, error) {
	if err := s.loadMetadata(); err != nil {
		return nil, err
	}

	s.metaMu.RLock()

	if s.filter != nil && !s.filter.MayContain([]byte(key)) {
		s.metaMu.RUnlock()
		return nil, ErrNotFound
	}

	if len(s.index) == 0 {
		s.metaMu.RUnlock()
		return nil, ErrNotFound
	}

	// find the block that contains the key using binary search
	i := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].FirstKey > key
	}) - 1

	if i < 0 {
		s.metaMu.RUnlock()
		return nil, ErrNotFound
	}

	entry := s.index[i]
	s.metaMu.RUnlock()

	// read the physical block from the file
	physical, err := s.readBlock(entry)
	if err != nil {
		return nil, err
	}

	// decode the block into records
	records, err := decodeBlock(physical)
	if err != nil {
		return nil, err
	}

	// search for the key in the records
	for _, rec := range records {
		if rec.Key == key {
			if rec.Deleted {
				return nil, ErrDeleted
			}
			return rec.Value, nil
		}

		if rec.Key > key {
			return nil, ErrNotFound
		}
	}

	return nil, ErrNotFound
}

func (s *SSTable) MaxSeq() (uint64, error) {
	it, err := s.Iterator()
	if err != nil {
		return 0, err
	}
	defer it.Close()

	var maxSeq uint64

	for it.Next() {
		rec := it.Record()

		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
	}

	if err := it.Err(); err != nil {
		return 0, err
	}

	return maxSeq, nil
}

func (s *SSTable) loadMetadata() error {
	s.metaMu.RLock()
	loaded := s.index != nil
	s.metaMu.RUnlock()

	if loaded {
		return nil
	}

	s.metaMu.Lock()
	defer s.metaMu.Unlock()

	// A go routine may have loaded the index while waiting for lock
	if s.index != nil {
		return nil
	}

	file, err := os.Open(s.Path)
	if err != nil {
		return err
	}
	defer file.Close()

	// parse the footer
	footer, err := readFooter(file)
	if err != nil {
		return err
	}

	indexBuf := make([]byte, footer.indexLength)
	if _, err := file.ReadAt(indexBuf, int64(footer.indexOffset)); err != nil {
		return fmt.Errorf(
			"%w: read index at offset %d: %v",
			ErrCorruptSSTable,
			footer.indexOffset,
			err,
		)
	}

	index, err := decodeIndex(indexBuf)
	if err != nil {
		return fmt.Errorf(
			"%w: decode index at offset %d: %v",
			ErrCorruptSSTable,
			footer.indexOffset,
			err,
		)
	}

	var filter *bloom.Filter

	if footer.filterLength > 0 {
		filterBuf := make([]byte, footer.filterLength)

		if _, err := file.ReadAt(
			filterBuf,
			int64(footer.filterOffset),
		); err != nil {
			return fmt.Errorf(
				"%w: read bloom filter at offset %d: %v",
				ErrCorruptSSTable,
				footer.filterOffset,
				err,
			)
		}

		filter, err = bloom.Decode(filterBuf)
		if err != nil {
			return fmt.Errorf(
				"%w: decode bloom filter at offset %d: %v",
				ErrCorruptSSTable,
				footer.filterOffset,
				err,
			)
		}
	}

	s.index = index
	s.filter = filter
	return nil
}
