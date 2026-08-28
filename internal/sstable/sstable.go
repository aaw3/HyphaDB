package sstable

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/aaw3/hyphadb/internal/blockcache"
	"github.com/aaw3/hyphadb/internal/bloom"
	"github.com/aaw3/hyphadb/internal/record"
)

var ErrNotFound = errors.New("key not found")
var ErrDeleted = errors.New("key has been deleted")
var ErrUnsortedRecords = errors.New("records are not sorted")
var ErrRetired = errors.New("SSTable is retired")

type SSTable struct {
	ID          uint64
	Level       uint32
	Path        string
	SizeBytes   uint64
	SmallestKey string
	LargestKey  string

	cache blockcache.Cache

	metaMu     sync.RWMutex
	metaLoaded bool
	index      []IndexEntry
	filter     *bloom.Filter

	refMu    sync.Mutex
	refs     int
	retired  bool
	noRefsCh chan struct{}
}

type OpenOptions struct {
	ID          uint64
	Level       uint32
	SizeBytes   uint64
	SmallestKey string
	LargestKey  string
	BlockCache  blockcache.Cache
}

func New(path string, opts OpenOptions) *SSTable {
	return &SSTable{
		ID:          opts.ID,
		Level:       opts.Level,
		Path:        path,
		SizeBytes:   opts.SizeBytes,
		SmallestKey: opts.SmallestKey,
		LargestKey:  opts.LargestKey,
		cache:       opts.BlockCache,
		noRefsCh:    make(chan struct{}),
	}
}

// Acquire retains the SSTable for a reader or background operation.
func (s *SSTable) Acquire() error {
	s.refMu.Lock()
	defer s.refMu.Unlock()
	if s.retired {
		return ErrRetired
	}
	if s.refs == 0 {
		s.noRefsCh = make(chan struct{})
	}
	s.refs++
	return nil
}

// Release drops a previously acquired SSTable reference.
func (s *SSTable) Release() {
	s.refMu.Lock()
	defer s.refMu.Unlock()
	if s.refs == 0 {
		return
	}
	s.refs--
	if s.refs == 0 {
		close(s.noRefsCh)
	}
}

// Retire prevents new references and returns a channel closed after existing
// readers release the SSTable.
func (s *SSTable) Retire() <-chan struct{} {
	s.refMu.Lock()
	defer s.refMu.Unlock()
	if s.retired {
		return s.noRefsCh
	}
	s.retired = true
	if s.refs == 0 {
		close(s.noRefsCh)
	}
	return s.noRefsCh
}

func (s *SSTable) Get(key string) ([]byte, error) {
	rec, ok, err := s.GetRecordAt(key, ^uint64(0))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	if rec.Deleted {
		return nil, ErrDeleted
	}
	return rec.Value, nil
}

// GetRecordAt returns the newest version of key with sequence <= maxSeq.
func (s *SSTable) GetRecordAt(key string, maxSeq uint64) (record.Record, bool, error) {
	if err := s.loadMetadata(); err != nil {
		return record.Record{}, false, err
	}

	s.metaMu.RLock()

	if s.filter != nil && !s.filter.MayContain([]byte(key)) {
		s.metaMu.RUnlock()
		return record.Record{}, false, nil
	}

	if len(s.index) == 0 {
		s.metaMu.RUnlock()
		return record.Record{}, false, nil
	}
	s.metaMu.RUnlock()

	// Find the first block that may contain key. Continue through subsequent
	// blocks because multiple versions of one key may cross a block boundary.
	i := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].FirstKey > key
	}) - 1
	if i < 0 {
		return record.Record{}, false, nil
	}

	for ; i < len(s.index); i++ {
		logical, err := s.readLogicalBlock(s.index[i])
		if err != nil {
			return record.Record{}, false, err
		}
		records, err := decodeLogicalBlock(logical)
		if err != nil {
			return record.Record{}, false, err
		}

		for _, rec := range records {
			if rec.Key == key && rec.Seq <= maxSeq {
				return rec, true, nil
			}
			if rec.Key > key {
				return record.Record{}, false, nil
			}
		}
	}
	return record.Record{}, false, nil
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
