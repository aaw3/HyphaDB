package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

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
	if len(buf) < 4 {
		return nil, fmt.Errorf(
			"%w: logical block missing record count",
			ErrCorruptSSTable,
		)
	}

	r := bytes.NewReader(buf)

	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, fmt.Errorf(
			"%w: read record count: %w",
			ErrCorruptSSTable,
			err,
		)
	}

	count := binary.LittleEndian.Uint32(countBuf[:])

	if uint64(count) > uint64(r.Len())/uint64(record.HeaderSize) {
		return nil, fmt.Errorf(
			"%w: record count %d cannot fit in block with %d remaining bytes",
			ErrCorruptSSTable,
			count,
			r.Len(),
		)
	}

	records := make([]record.Record, 0, count)

	// decode each record in the block
	for i := uint32(0); i < count; i++ {
		rec, err := record.DecodeBinary(r)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: decode record %d: %w",
				ErrCorruptSSTable,
				i,
				err,
			)
		}

		records = append(records, rec)
	}

	if r.Len() != 0 {
		return nil, fmt.Errorf(
			"%w: block has %d unexpected trailing bytes",
			ErrCorruptSSTable,
			r.Len(),
		)
	}

	return records, nil
}

func decodeBlock(physical []byte) ([]record.Record, error) {
	logical, err := decodePhysicalBlock(physical)
	if err != nil {
		return nil, err
	}

	return decodeLogicalBlock(logical)
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
