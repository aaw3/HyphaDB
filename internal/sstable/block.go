package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/aaw3/hyphadb/internal/record"
)

type CompressionType byte

const (
	CompressionNone CompressionType = iota
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
	Codec  CompressionType
	RawLen uint32
}

func encodePhysicalBlock(logical []byte, compression CompressionType) ([]byte, error) {
	if len(logical) > maxBlockSize {
		return nil, fmt.Errorf(
			"%w: block size %d exceeds maximum %d",
			ErrCorruptSSTable,
			len(logical),
			maxBlockSize,
		)
	}

	header := BlockHeader{
		Codec:  compression,
		RawLen: uint32(len(logical)),
	}

	var storedPayload []byte

	switch compression {
	case CompressionNone:
		storedPayload = logical

	default:
		return nil, fmt.Errorf(
			"%w: unknown compression type %d",
			ErrCorruptSSTable,
			compression,
		)
	}

	physicalSize := blockHeaderSize + len(storedPayload) + blockTrailerSize
	physical := make([]byte, physicalSize)

	physical[0] = byte(header.Codec)
	binary.LittleEndian.PutUint32(physical[1:5], header.RawLen)

	copy(physical[blockHeaderSize:], storedPayload)

	checksumStart := physicalSize - blockTrailerSize
	checksum := crc32.Checksum(physical[:checksumStart], crc32cTable)

	binary.LittleEndian.PutUint32(physical[checksumStart:], checksum)

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
		Codec:  CompressionType(physical[0]),
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

	switch header.Codec {
	case CompressionNone:
		if uint64(header.RawLen) != uint64(len(storedPayload)) {
			return nil, fmt.Errorf(
				"%w: raw block length %d does not match payload length %d",
				ErrCorruptSSTable,
				header.RawLen,
				len(storedPayload),
			)
		}

		return storedPayload, nil
	default:
		return nil, fmt.Errorf(
			"%w: unknown compression type %d",
			ErrCorruptSSTable,
			header.Codec,
		)
	}
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
