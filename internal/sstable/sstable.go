package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
)

var ErrNotFound = errors.New("key not found")
var ErrDeleted = errors.New("key has been deleted")

const (
	DefaultBlockSize = 64 * 1024 // 64KB
	footerSize       = 24
)

var magic = [8]byte{'H', 'Y', 'P', 'H', 'S', 'S', 'T', '1'}

type SSTable struct {
	Path  string
	index []IndexEntry
}

type IndexEntry struct {
	FirstKey string
	Offset   uint64
	Length   uint32
}

func CreateFromMemTable(mt *memtable.MemTable, path string) (*SSTable, error) {
	return CreateFromRecords(mt.Records(), path, DefaultBlockSize)
}

func CreateFromRecords(records []record.Record, path string, blockSize int) (*SSTable, error) {
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
		physical, err := encodePhysicalBlock(logical, CompressionNone)
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

	for _, rec := range records {
		if recordCount == 0 {
			startBlock(rec.Key)
		}

		// get the size of the record when encoded
		recSize := record.EncodedSize(rec)

		// flush the block if adding this record would exceed the block size
		if recordCount > 0 && logicalBlock.Len()+recSize > blockSize {
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

	// write the footer at the end of the file
	if err := writeFooter(file, uint64(indexOffset), indexLength); err != nil {
		return nil, err
	}

	return &SSTable{
		Path:  path,
		index: index,
	}, nil
}

func (s *SSTable) Open(key string) ([]byte, error) {
	if err := s.loadIndex(); err != nil {
		return nil, err
	}

	if len(s.index) == 0 {
		return nil, ErrNotFound
	}

	// find the block that contains the key using binary search
	i := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].FirstKey > key
	}) - 1

	if i < 0 {
		return nil, ErrNotFound
	}

	// read the physical block from the file
	physical, err := s.readBlock(s.index[i])
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
			if rec.Entry.Deleted {
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

func (s *SSTable) loadIndex() error {
	if s.index != nil {
		return nil
	}

	file, err := os.Open(s.Path)
	if err != nil {
		return err
	}
	defer file.Close()

	// parse the footer
	indexOffset, indexLength, err := readFooter(file)
	if err != nil {
		return err
	}

	if _, err := file.Seek(int64(indexOffset), io.SeekStart); err != nil {
		return err
	}

	// read the index from the file
	buf := make([]byte, indexLength)
	if _, err := io.ReadFull(file, buf); err != nil {
		return err
	}

	index, err := decodeIndex(buf)
	if err != nil {
		return err
	}

	s.index = index
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

func writeFooter(w io.Writer, indexOffset uint64, indexLength uint64) error {
	var footer [footerSize]byte

	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], indexLength)
	copy(footer[16:], magic[:])

	_, err := w.Write(footer[:])
	return err
}
func readFooter(file *os.File) (uint64, uint64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}

	fileSize := uint64(info.Size())

	if fileSize < footerSize {
		return 0, 0, errors.New("sstable too small")
	}

	if _, err := file.Seek(-footerSize, io.SeekEnd); err != nil {
		return 0, 0, err
	}

	var footer [footerSize]byte
	if _, err := io.ReadFull(file, footer[:]); err != nil {
		return 0, 0, err
	}

	if !bytes.Equal(footer[16:], magic[:]) {
		return 0, 0, errors.New("invalid sstable magic number")
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexLength := binary.LittleEndian.Uint64(footer[8:16])

	if indexOffset > fileSize-uint64(footerSize) {
		return 0, 0, errors.New("invalid index offset")
	}

	if indexLength > fileSize-uint64(footerSize)-indexOffset {
		return 0, 0, errors.New("invalid index length")
	}

	return indexOffset, indexLength, nil
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
