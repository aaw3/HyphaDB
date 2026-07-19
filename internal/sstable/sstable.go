package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/aaw3/hyphadb/internal/bloom"
	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
)

var ErrNotFound = errors.New("key not found")
var ErrDeleted = errors.New("key has been deleted")
var ErrUnsortedRecords = errors.New("records are not sorted")

const (
	DefaultBlockSize = 64 * 1024 // 64KB
	footerSize       = 40

	currentFormatVersion = 2

	DefaultMinCompressionSavingsRate = 0.125
)

var tableMagic = [6]byte{'H', 'Y', 'P', 'S', 'S', 'T'}

type SSTable struct {
	Path string

	metaMu sync.RWMutex
	index  []IndexEntry
	filter *bloom.Filter
}

type IndexEntry struct {
	FirstKey string
	Offset   uint64
	Length   uint32
}

type footerMetadata struct {
	indexOffset  uint64
	indexLength  uint64
	filterOffset uint64
	filterLength uint64
}

func CreateFromMemTable(mt *memtable.MemTable, path string) (*SSTable, error) {
	return CreateFromRecordsWithOptions(mt.Records(), path, DefaultWriteOptions())
}

func CreateFromRecords(
	records []record.Record,
	path string,
	blockSize int,
) (*SSTable, error) {
	opts := DefaultWriteOptions()
	opts.BlockSize = blockSize

	return CreateFromRecordsWithOptions(records, path, opts)
}

func CreateFromRecordsWithOptions(
	records []record.Record,
	path string,
	opts WriteOptions,
) (*SSTable, error) {
	opts, err := normalizeWriteOptions(opts)
	if err != nil {
		return nil, err
	}

	var filter *bloom.Filter

	bloomFilterEnabled := opts.Bloom.Enabled && len(records) > 0

	if bloomFilterEnabled {
		filter, err = bloom.New(len(records), opts.Bloom.FalsePositiveRate)
		if err != nil {
			return nil, fmt.Errorf("failed to create bloom filter: %w", err)
		}
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var index []IndexEntry
	var logicalBlock bytes.Buffer
	var recordCount int
	var blockFirstKey string

	// closure to start a new block
	startBlock := func(firstKey string) {
		logicalBlock.Reset()

		var countPlaceholder [4]byte
		logicalBlock.Write(countPlaceholder[:])

		recordCount = 0
		blockFirstKey = firstKey
	}

	flushBlock := func() error {
		if recordCount == 0 {
			return nil
		}

		// write record count at the beginning of the block
		binary.LittleEndian.PutUint32(logicalBlock.Bytes()[0:4], uint32(recordCount))

		logical := logicalBlock.Bytes()
		physical, err := encodePhysicalBlock(logical, opts.Compression, opts.MinSavingsRate)
		if err != nil {
			return err
		}

		// get the current offset in the file
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		// write the physical block to the SSTable file
		if _, err := file.Write(physical); err != nil {
			return err
		}

		// add the index pointing to the physical block
		index = append(index, IndexEntry{
			FirstKey: blockFirstKey,
			Offset:   uint64(offset),
			Length:   uint32(len(physical)),
		})

		logicalBlock.Reset()
		recordCount = 0
		blockFirstKey = ""

		return nil
	}

	for i, rec := range records {
		if i > 0 && records[i-1].Key > rec.Key {
			return nil, fmt.Errorf(
				"%w: key %q appears before %q",
				ErrUnsortedRecords,
				records[i-1].Key,
				rec.Key,
			)
		}

		if bloomFilterEnabled {
			filter.Add([]byte(rec.Key))
		}

		if recordCount == 0 {
			startBlock(rec.Key)
		}

		// get the size of the record when encoded
		recSize := record.EncodedSize(rec)

		// flush the block if adding this record would exceed the block size
		if recordCount > 0 && logicalBlock.Len()+recSize > opts.BlockSize {
			if err := flushBlock(); err != nil {
				return nil, err
			}
			startBlock(rec.Key)
		}

		if err := record.EncodeBinary(&logicalBlock, rec); err != nil {
			return nil, err
		}
		recordCount++
	}

	if err := flushBlock(); err != nil {
		return nil, err
	}

	indexOffset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	// write the index after the blocks and before the footer
	if err := writeIndex(file, index); err != nil {
		return nil, err
	}

	indexEnd, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	indexLength := uint64(indexEnd - indexOffset)

	var filterOffset uint64
	var filterLength uint64

	if filter != nil {
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		encodedFilter, err := filter.Encode()
		if err != nil {
			return nil, fmt.Errorf("failed to encode bloom filter: %w", err)
		}

		if _, err := file.Write(encodedFilter); err != nil {
			return nil, fmt.Errorf("failed to write bloom filter: %w", err)
		}

		filterOffset = uint64(offset)
		filterLength = uint64(len(encodedFilter))
	}

	// write the footer at the end of the file
	if err := writeFooter(
		file,
		uint64(indexOffset),
		indexLength,
		filterOffset,
		filterLength,
	); err != nil {
		return nil, err
	}

	return &SSTable{
		Path:   path,
		index:  index,
		filter: filter,
	}, nil
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

	if _, err := file.Seek(int64(footer.indexOffset), io.SeekStart); err != nil {
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

func (s *SSTable) readBlock(entry IndexEntry) ([]byte, error) {
	file, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return readBlockFrom(file, entry)
}

// keep file open for reading blocks, to avoid reopening the file for each block read
func readBlockFrom(file *os.File, entry IndexEntry) ([]byte, error) {
	maxStoredBlockSize := uint64(maxBlockSize) + uint64(blockHeaderSize) + uint64(blockTrailerSize)

	if entry.Length == 0 {
		return nil, fmt.Errorf(
			"%w: block at offset %d has zero length",
			ErrCorruptSSTable,
			entry.Offset,
		)
	}
	if uint64(entry.Length) > maxStoredBlockSize {
		return nil, fmt.Errorf(
			"%w: block at offset %d has length %d exceeding maximum stored block size %d",
			ErrCorruptSSTable,
			entry.Offset,
			entry.Length,
			maxStoredBlockSize,
		)
	}

	if _, err := file.Seek(int64(entry.Offset), io.SeekStart); err != nil {
		return nil, err
	}

	buf := make([]byte, entry.Length)
	if _, err := io.ReadFull(file, buf); err != nil {
		return nil, fmt.Errorf(
			"%w: read block at offset %d: %v",
			ErrCorruptSSTable,
			entry.Offset,
			err,
		)
	}

	return buf, nil
}

func writeFooter(
	w io.Writer,
	indexOffset uint64,
	indexLength uint64,
	filterOffset uint64,
	filterLength uint64,
) error {
	var footer [footerSize]byte

	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], indexLength)
	binary.LittleEndian.PutUint64(footer[16:24], filterOffset)
	binary.LittleEndian.PutUint64(footer[24:32], filterLength)

	copy(footer[32:38], tableMagic[:])
	footer[38] = currentFormatVersion
	footer[39] = 0 // reserved flags byte

	_, err := w.Write(footer[:])
	return err
}

func readFooter(file *os.File) (footerMetadata, error) {
	info, err := file.Stat()
	if err != nil {
		return footerMetadata{}, err
	}

	fileSize := uint64(info.Size())

	if fileSize < footerSize {
		return footerMetadata{}, fmt.Errorf("%w: SSTable too small",
			ErrCorruptSSTable,
		)
	}

	if _, err := file.Seek(-footerSize, io.SeekEnd); err != nil {
		return footerMetadata{}, err
	}

	var footer [footerSize]byte
	if _, err := io.ReadFull(file, footer[:]); err != nil {
		return footerMetadata{}, fmt.Errorf(
			"%w: read footer: %v",
			ErrCorruptSSTable,
			err,
		)
	}

	if !bytes.Equal(footer[32:38], tableMagic[:]) {
		return footerMetadata{}, fmt.Errorf(
			"%w: invalid SSTable magic string",
			ErrCorruptSSTable,
		)
	}

	version := footer[38]
	if version != currentFormatVersion {
		return footerMetadata{}, fmt.Errorf(
			"%w: unsupported SSTable format version: %d",
			ErrCorruptSSTable,
			version,
		)
	}

	flags := footer[39]
	if flags != 0 { // reserved for future use, only zero is supported currently
		return footerMetadata{}, fmt.Errorf(
			"%w: unsupported sstable footer flags: %#x",
			ErrCorruptSSTable,
			flags,
		)
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexLength := binary.LittleEndian.Uint64(footer[8:16])
	filterOffset := binary.LittleEndian.Uint64(footer[16:24])
	filterLength := binary.LittleEndian.Uint64(footer[24:32])

	dataEnd := fileSize - uint64(footerSize)

	if indexOffset > dataEnd {
		return footerMetadata{}, fmt.Errorf(
			"%w: index offset %d exceeds metadata boundary %d",
			ErrCorruptSSTable,
			indexOffset,
			dataEnd,
		)
	}

	if indexLength > dataEnd-indexOffset {
		return footerMetadata{}, fmt.Errorf(
			"%w: index length %d exceeds data end %d minus index offset %d",
			ErrCorruptSSTable,
			indexLength,
			dataEnd,
			indexOffset,
		)
	}

	indexEnd := indexOffset + indexLength

	if filterLength == 0 {
		if filterOffset != 0 {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter length is zero but filter offset is non-zero (%d)",
				ErrCorruptSSTable,
				filterOffset,
			)
		}

		if indexEnd != dataEnd {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter length is zero but index end %d does not match data end %d",
				ErrCorruptSSTable,
				indexEnd,
				dataEnd,
			)
		}
	} else {
		if filterOffset != indexEnd {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter offset %d does not match index end %d",
				ErrCorruptSSTable,
				filterOffset,
				indexEnd,
			)
		}

		if filterOffset > dataEnd {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter offset %d exceeds metadata boundary %d",
				ErrCorruptSSTable,
				filterOffset,
				dataEnd,
			)
		}

		if filterLength > dataEnd-filterOffset {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter length %d exceeds data end %d minus filter offset %d",
				ErrCorruptSSTable,
				filterLength,
				dataEnd,
				filterOffset,
			)
		}

		if filterOffset+filterLength != dataEnd {
			return footerMetadata{}, fmt.Errorf(
				"%w: filter end %d does not match data end %d",
				ErrCorruptSSTable,
				filterOffset+filterLength,
				dataEnd,
			)
		}
	}

	return footerMetadata{
		indexOffset,
		indexLength,
		filterOffset,
		filterLength,
	}, nil
}

func writeIndex(w io.Writer, index []IndexEntry) error {
	var buf bytes.Buffer
	var count [4]byte

	binary.LittleEndian.PutUint32(count[:], uint32(len(index)))
	buf.Write(count[:])

	for _, entry := range index {
		var header [16]byte

		binary.LittleEndian.PutUint32(header[0:4], uint32(len(entry.FirstKey)))
		binary.LittleEndian.PutUint64(header[4:12], entry.Offset)
		binary.LittleEndian.PutUint32(header[12:16], entry.Length)

		buf.Write(header[:])
		buf.WriteString(entry.FirstKey)
	}

	_, err := w.Write(buf.Bytes())
	return err
}

func decodeIndex(buf []byte) ([]IndexEntry, error) {
	r := bytes.NewReader(buf)

	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, err
	}

	count := binary.LittleEndian.Uint32(countBuf[:])
	index := make([]IndexEntry, 0, count)

	// read each index entry
	for i := uint32(0); i < count; i++ {
		var header [16]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return nil, err
		}

		keyLen := binary.LittleEndian.Uint32(header[0:4])
		offset := binary.LittleEndian.Uint64(header[4:12])
		length := binary.LittleEndian.Uint32(header[12:16])

		if uint64(r.Len()) < uint64(keyLen) {
			return nil, errors.New("invalid index key length")
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, err
		}

		// add the index entry to the slice
		index = append(index, IndexEntry{
			FirstKey: string(key),
			Offset:   offset,
			Length:   length,
		})
	}

	return index, nil
}
