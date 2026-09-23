package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"unsafe"

	"github.com/aaw3/hyphadb/internal/blockcache"
	"github.com/aaw3/hyphadb/internal/compression"
	"github.com/aaw3/hyphadb/internal/record"
)

const (
	blockHeaderSize  = 1 + 4
	blockTrailerSize = 4

	maxBlockSize = 256 * 1024 * 1024 // 256MB
)

var (
	ErrCorruptSSTable = errors.New("corrupt SSTable")
	crc32cTable       = crc32.MakeTable(crc32.Castagnoli)
)

type BlockHeader struct {
	Codec  compression.Type
	RawLen uint32
}

func encodePhysicalBlock(
	logical []byte,
	reqCodec compression.Type,
	minSavingsRate float64,
) ([]byte, error) {
	if len(logical) > maxBlockSize {
		return nil, fmt.Errorf(
			"%w: block size %d exceeds maximum %d",
			ErrCorruptSSTable,
			len(logical),
			maxBlockSize,
		)
	}

	storedPayload := logical
	actualCodec := compression.None

	if reqCodec != compression.None {
		compressed, err := compression.Compress(
			logical,
			reqCodec,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: compress block: %w",
				ErrCorruptSSTable,
				err,
			)
		}

		if shouldCompress(
			len(logical),
			len(compressed),
			minSavingsRate,
		) {
			storedPayload = compressed
			actualCodec = reqCodec
		}
	}

	header := BlockHeader{
		Codec:  actualCodec,
		RawLen: uint32(len(logical)),
	}

	physicalSize := blockHeaderSize + len(storedPayload) + blockTrailerSize
	physical := make([]byte, physicalSize)

	physical[0] = byte(header.Codec)
	binary.LittleEndian.PutUint32(physical[1:5], header.RawLen)

	copy(physical[blockHeaderSize:], storedPayload)

	checksumOffset := physicalSize - blockTrailerSize
	checksum := crc32.Checksum(physical[:checksumOffset], crc32cTable)

	binary.LittleEndian.PutUint32(physical[checksumOffset:], checksum)

	return physical, nil
}

func decodePhysicalBlock(physical []byte) ([]byte, error) {
	if len(physical) < blockHeaderSize+blockTrailerSize {
		return nil, fmt.Errorf(
			"%w: physical block is too small",
			ErrCorruptSSTable,
		)
	}

	checksumOffset := len(physical) - blockTrailerSize
	wantChecksum := binary.LittleEndian.Uint32(physical[checksumOffset:])
	gotChecksum := crc32.Checksum(physical[:checksumOffset], crc32cTable)

	if wantChecksum != gotChecksum {
		return nil, fmt.Errorf(
			"%w: block checksum mismatch",
			ErrCorruptSSTable,
		)
	}

	header := BlockHeader{
		Codec:  compression.Type(physical[0]),
		RawLen: binary.LittleEndian.Uint32(physical[1:5]),
	}

	if header.RawLen > maxBlockSize {
		return nil, fmt.Errorf(
			"%w: raw block length %d exceeds maximum %d",
			ErrCorruptSSTable,
			header.RawLen,
			maxBlockSize,
		)
	}

	storedPayload := physical[blockHeaderSize:checksumOffset]

	logical, err := compression.Decompress(
		storedPayload,
		header.RawLen,
		header.Codec,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: decompress block: %w",
			ErrCorruptSSTable,
			err,
		)
	}

	return logical, nil
}

func decodeLogicalBlock(buf []byte) ([]record.Record, error) {
	cursor, err := newLogicalBlockCursor(buf)
	if err != nil {
		return nil, err
	}

	records := make([]record.Record, 0, cursor.count)
	for {
		rec, ok := cursor.next()
		if !ok {
			break
		}
		records = append(records, rec)
	}
	return records, cursor.err
}

// logicalBlockCursor walks records directly from an immutable logical block.
// Records returned by next reference data owned by the block, so the block must
// remain alive and must not be modified while those records are in use.
type logicalBlockCursor struct {
	data            []byte
	count           uint32
	index           uint32
	offset          int
	recordsEnd      int
	restartInterval uint32
	restartOffsets  []byte
	err             error
}

func newLogicalBlockCursor(buf []byte) (logicalBlockCursor, error) {
	return newLogicalBlockCursorForVersion(buf, currentFormatVersion)
}

func newLogicalBlockCursorForVersion(
	buf []byte,
	formatVersion byte,
) (logicalBlockCursor, error) {
	if len(buf) < 4 {
		return logicalBlockCursor{}, fmt.Errorf(
			"%w: logical block missing record count",
			ErrCorruptSSTable,
		)
	}

	count := binary.LittleEndian.Uint32(buf[:4])
	recordsEnd := len(buf)
	var restartInterval uint32
	var restartOffsets []byte

	switch formatVersion {
	case legacyFormatVersion:
	case currentFormatVersion:
		if len(buf) < 12 {
			return logicalBlockCursor{}, fmt.Errorf(
				"%w: restart block is too small",
				ErrCorruptSSTable,
			)
		}

		restartCount := binary.LittleEndian.Uint32(buf[len(buf)-4:])
		metadataSize := uint64(restartCount)*4 + 8
		if metadataSize > uint64(len(buf)-4) {
			return logicalBlockCursor{}, fmt.Errorf(
				"%w: restart metadata exceeds block size",
				ErrCorruptSSTable,
			)
		}

		recordsEnd = len(buf) - int(metadataSize)
		restartOffsets = buf[recordsEnd : len(buf)-8]
		restartInterval = binary.LittleEndian.Uint32(buf[len(buf)-8 : len(buf)-4])
		if restartInterval == 0 {
			return logicalBlockCursor{}, fmt.Errorf(
				"%w: restart interval is zero",
				ErrCorruptSSTable,
			)
		}

		expectedRestarts := uint64(0)
		if count > 0 {
			expectedRestarts = (uint64(count) + uint64(restartInterval) - 1) /
				uint64(restartInterval)
		}
		if uint64(restartCount) != expectedRestarts {
			return logicalBlockCursor{}, fmt.Errorf(
				"%w: restart count %d does not match record count %d and interval %d",
				ErrCorruptSSTable,
				restartCount,
				count,
				restartInterval,
			)
		}

		var previous uint32
		for i := uint32(0); i < restartCount; i++ {
			offset := binary.LittleEndian.Uint32(restartOffsets[i*4:])
			if offset < 4 || uint64(offset) >= uint64(recordsEnd) {
				return logicalBlockCursor{}, fmt.Errorf(
					"%w: restart offset %d is outside record data",
					ErrCorruptSSTable,
					offset,
				)
			}
			if i == 0 && offset != 4 {
				return logicalBlockCursor{}, fmt.Errorf(
					"%w: first restart offset is %d, want 4",
					ErrCorruptSSTable,
					offset,
				)
			}
			if i > 0 && offset <= previous {
				return logicalBlockCursor{}, fmt.Errorf(
					"%w: restart offsets are not increasing",
					ErrCorruptSSTable,
				)
			}
			previous = offset
		}
	default:
		return logicalBlockCursor{}, fmt.Errorf(
			"%w: unsupported block format version: %d",
			ErrCorruptSSTable,
			formatVersion,
		)
	}

	remaining := recordsEnd - 4
	if uint64(count) > uint64(remaining)/uint64(record.HeaderSize) {
		return logicalBlockCursor{}, fmt.Errorf(
			"%w: record count %d cannot fit in block with %d remaining bytes",
			ErrCorruptSSTable,
			count,
			remaining,
		)
	}

	return logicalBlockCursor{
		data:            buf,
		count:           count,
		offset:          4,
		recordsEnd:      recordsEnd,
		restartInterval: restartInterval,
		restartOffsets:  restartOffsets,
	}, nil
}

func (c *logicalBlockCursor) seek(key string) error {
	if c.err != nil || len(c.restartOffsets) == 0 {
		return c.err
	}

	restartCount := len(c.restartOffsets) / 4
	var searchErr error
	i := sort.Search(restartCount, func(i int) bool {
		offset := c.restartOffset(i)
		rec, _, err := decodeRecordView(c.data[offset:c.recordsEnd])
		if err != nil {
			searchErr = err
			return true
		}
		return rec.Key >= key
	})
	if searchErr != nil {
		c.err = fmt.Errorf("%w: decode restart record: %v", ErrCorruptSSTable, searchErr)
		return c.err
	}
	if i == restartCount {
		i = restartCount - 1
	} else if i > 0 {
		i--
	}

	c.offset = c.restartOffset(i)
	c.index = uint32(i) * c.restartInterval
	return nil
}

func (c *logicalBlockCursor) restartOffset(index int) int {
	start := index * 4
	return int(binary.LittleEndian.Uint32(c.restartOffsets[start : start+4]))
}

func (c *logicalBlockCursor) next() (record.Record, bool) {
	if c.err != nil {
		return record.Record{}, false
	}

	if c.index == c.count {
		if c.offset != c.recordsEnd {
			c.err = fmt.Errorf(
				"%w: block has %d unexpected trailing bytes",
				ErrCorruptSSTable,
				c.recordsEnd-c.offset,
			)
		}
		return record.Record{}, false
	}

	rec, size, err := decodeRecordView(c.data[c.offset:c.recordsEnd])
	if err != nil {
		c.err = fmt.Errorf(
			"%w: decode record %d: %v",
			ErrCorruptSSTable,
			c.index,
			err,
		)
		return record.Record{}, false
	}

	c.offset += size
	c.index++
	return rec, true
}

// decodeRecordView decodes one record without copying its key or value. The
// caller owns the lifetime and immutability of buf.
func decodeRecordView(buf []byte) (record.Record, int, error) {
	if len(buf) < record.HeaderSize {
		return record.Record{}, 0, io.ErrUnexpectedEOF
	}

	keyLen := binary.LittleEndian.Uint32(buf[0:4])
	valueLen := binary.LittleEndian.Uint32(buf[4:8])
	seq := binary.LittleEndian.Uint64(buf[8:16])
	flags := buf[16]

	if flags&^record.FlagDeleted != 0 {
		return record.Record{}, 0, fmt.Errorf(
			"unknown record flags: %08b",
			flags,
		)
	}
	if keyLen > record.MaxKeySize {
		return record.Record{}, 0, fmt.Errorf(
			"key length %d exceeds maximum allowed size %d",
			keyLen,
			record.MaxKeySize,
		)
	}
	if valueLen > record.MaxValueSize {
		return record.Record{}, 0, fmt.Errorf(
			"value length %d exceeds maximum allowed size %d",
			valueLen,
			record.MaxValueSize,
		)
	}

	payloadLen := uint64(keyLen) + uint64(valueLen)
	if payloadLen > uint64(len(buf)-record.HeaderSize) {
		return record.Record{}, 0, fmt.Errorf(
			"record requires %d payload bytes, but only %d bytes remain",
			payloadLen,
			len(buf)-record.HeaderSize,
		)
	}

	keyStart := record.HeaderSize
	keyEnd := keyStart + int(keyLen)
	valueEnd := keyEnd + int(valueLen)
	keyBytes := buf[keyStart:keyEnd]

	var key string
	if len(keyBytes) > 0 {
		key = unsafe.String(unsafe.SliceData(keyBytes), len(keyBytes))
	}

	var value []byte
	if valueLen > 0 {
		value = buf[keyEnd:valueEnd]
	}

	return record.Record{
		Key: key,
		Seq: seq,
		Entry: record.Entry{
			Value:   value,
			Deleted: flags&record.FlagDeleted != 0,
		},
	}, valueEnd, nil
}

func decodeBlock(physical []byte) ([]record.Record, error) {
	logical, err := decodePhysicalBlock(physical)
	if err != nil {
		return nil, err
	}

	return decodeLogicalBlock(logical)
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

	if entry.Offset > math.MaxInt64 {
		return nil, fmt.Errorf(
			"%w: block at offset %d exceeds max int64",
			ErrCorruptSSTable,
			entry.Offset,
		)
	}

	buf := make([]byte, entry.Length)
	if _, err := file.ReadAt(buf, int64(entry.Offset)); err != nil {
		return nil, fmt.Errorf(
			"%w: read block at offset %d: %v",
			ErrCorruptSSTable,
			entry.Offset,
			err,
		)
	}

	return buf, nil
}

func (s *SSTable) readLogicalBlock(entry IndexEntry) ([]byte, error) {
	cacheKey := s.blockCacheKey(entry)

	if s.cache != nil {
		if logical, ok := s.cache.Get(cacheKey); ok {
			return logical, nil
		}
	}

	file, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return s.readLogicalBlockMiss(file, entry, cacheKey)
}

func (s *SSTable) readLogicalBlockFrom(
	file *os.File,
	entry IndexEntry,
) ([]byte, error) {
	cacheKey := s.blockCacheKey(entry)

	if s.cache != nil {
		if logical, ok := s.cache.Get(cacheKey); ok {
			return logical, nil
		}
	}

	return s.readLogicalBlockMiss(file, entry, cacheKey)
}

func (s *SSTable) readLogicalBlockMiss(
	file *os.File,
	entry IndexEntry,
	cacheKey blockcache.Key,
) ([]byte, error) {
	physical, err := readBlockFrom(file, entry)
	if err != nil {
		return nil, err
	}

	logical, err := decodePhysicalBlock(physical)
	if err != nil {
		return nil, err
	}

	if s.cache != nil {
		s.cache.Set(cacheKey, logical)
	}

	return logical, nil
}

func (s *SSTable) blockCacheKey(entry IndexEntry) blockcache.Key {
	return blockcache.Key{
		TableID: s.ID,
		Offset:  entry.Offset,
	}
}

// ===================
// Compression Helper
// ===================

func shouldCompress(
	rawSize int,
	compressedSize int,
	minSavingsRate float64,
) bool {
	if rawSize <= 0 || compressedSize >= rawSize {
		return false
	}

	savingsRate :=
		float64(rawSize-compressedSize) / float64(rawSize)

	return savingsRate >= minSavingsRate
}
