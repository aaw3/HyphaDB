package sstable

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/aaw3/hyphadb/internal/blockcache"
	"github.com/aaw3/hyphadb/internal/bloom"
)

var ErrNotFound = errors.New("key not found")
var ErrDeleted = errors.New("key has been deleted")
var ErrUnsortedRecords = errors.New("records are not sorted")

type SSTable struct {
	ID   uint64
	Path string

	cache blockcache.Cache

	metaMu     sync.RWMutex
	metaLoaded bool
	index      []IndexEntry
	filter     *bloom.Filter
}

type OpenOptions struct {
	ID         uint64
	BlockCache blockcache.Cache
}

func New(path string, opts OpenOptions) *SSTable {
	return &SSTable{
		ID:    opts.ID,
		Path:  path,
		cache: opts.BlockCache,
	}
}

func (s *SSTable) Get(key string) ([]byte, error) {
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
	logical, err := s.readLogicalBlock(entry)
	if err != nil {
		return nil, err
	}

	// decode the block into records
	records, err := decodeLogicalBlock(logical)
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
	loaded := s.metaLoaded
	s.metaMu.RUnlock()

	if loaded {
		return nil
	}

	s.metaMu.Lock()
	defer s.metaMu.Unlock()

	// A go routine may have loaded the index while waiting for lock
	if s.metaLoaded {
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

	indexLength, err := checkedBufferLength(footer.indexLength, maxIndexSize, "index")
	if err != nil {
		return err
	}

	indexBuf := make([]byte, indexLength)
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

	filterLength, err := checkedBufferLength(footer.filterLength, maxFilterSize, "filter")
	if err != nil {
		return err
	}

	if filterLength > 0 {
		filterBuf := make([]byte, filterLength)

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
	s.metaLoaded = true
	return nil
}
