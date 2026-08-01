package sstable

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/aaw3/hyphadb/internal/compression"
	"github.com/aaw3/hyphadb/internal/record"
)

func TestCompressedSSTableRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compressed.sst")

	records := []record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: bytes.Repeat([]byte("red"), 1000),
			},
		},
		{
			Key: "banana",
			Seq: 2,
			Entry: record.Entry{
				Value: bytes.Repeat([]byte("yellow"), 1000),
			},
		},
	}

	opts := DefaultWriteOptions()
	sst, err := CreateFromRecordsWithOptions(
		records,
		path,
		WriteOptions{
			BlockSize:                 opts.BlockSize,
			Compression:               compression.LZ4,
			MinCompressionSavingsRate: opts.MinCompressionSavingsRate,
		},
	)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions error: %v", err)
	}

	got, err := sst.Get("banana")
	if err != nil {
		t.Fatalf("Get banana: %v", err)
	}

	if !bytes.Equal(got, records[1].Value) {
		t.Fatal("value mismatch")
	}
}
