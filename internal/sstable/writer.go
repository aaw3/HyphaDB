package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/aaw3/hyphadb/internal/bloom"
	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
)

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
		physical, err := encodePhysicalBlock(logical, opts.Compression, opts.MinCompressionSavingsRate)
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
