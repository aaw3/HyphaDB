package sstable

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aaw3/hyphadb/internal/compression"
	"github.com/aaw3/hyphadb/internal/record"
)

func TestCompressedSSTableCorruptionReturnsErrCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compressed.sst")

	records := []record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: bytes.Repeat([]byte("red"), 1000),
			},
		},
	}

	opts := DefaultWriteOptions()
	sst, err := CreateFromRecordsWithOptions(
		records,
		path,
		WriteOptions{
			BlockSize:                 DefaultBlockSize,
			Compression:               compression.LZ4,
			MinCompressionSavingsRate: opts.MinCompressionSavingsRate,
		},
	)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions failed: %v", err)
	}

	entry := sst.index[0]

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile error: %v", err)
	}

	offset := int64(entry.Offset) + int64(blockHeaderSize)

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		file.Close()
		t.Fatalf("Seek error: %v", err)
	}

	var b [1]byte
	if _, err := file.Read(b[:]); err != nil {
		file.Close()
		t.Fatalf("Read error: %v", err)
	}

	// Flip first byte in block payload
	b[0] ^= 0xff

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		file.Close()
		t.Fatalf("Seek rewrite: %v", err)
	}

	if _, err := file.Write(b[:]); err != nil {
		file.Close()
		t.Fatalf("Write: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := &SSTable{Path: path}

	_, err = reopened.Get("apple")
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrCorruptSSTable,
		)
	}
}
