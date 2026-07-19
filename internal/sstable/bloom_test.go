package sstable

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
)

// ===============
// Test helpers
// ===============
func createTestSSTableWithBloom(t *testing.T, path string) {
	t.Helper()

	records := []record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
		{
			Key: "banana",
			Seq: 2,
			Entry: record.Entry{
				Value: []byte("yellow"),
			},
		},
	}

	opts := DefaultWriteOptions()
	opts.Bloom.Enabled = true
	opts.Bloom.FalsePositiveRate = 0.01

	if _, err := CreateFromRecordsWithOptions(
		records,
		path,
		opts,
	); err != nil {
		t.Fatalf("CreateFromRecordsWithOptions: %v", err)
	}
}

// ================
// Tests
// ================

func TestBloomFilterPersistsAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	records := []record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
		{
			Key: "banana",
			Seq: 2,
			Entry: record.Entry{
				Value: []byte("yellow"),
			},
		},
		{
			Key: "orange",
			Seq: 3,
			Entry: record.Entry{
				Value: []byte("orange"),
			},
		},
	}

	opts := DefaultWriteOptions()
	opts.Bloom.Enabled = true
	opts.Bloom.FalsePositiveRate = 0.01

	_, err := CreateFromRecordsWithOptions(records, path, opts)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions: %v", err)
	}

	reopened := &SSTable{
		Path: path,
	}

	if err := reopened.loadMetadata(); err != nil {
		t.Fatalf("loadMetadata: %v", err)
	}

	reopened.metaMu.RLock()
	filter := reopened.filter
	reopened.metaMu.RUnlock()

	if filter == nil {
		t.Fatal("reopened SSTable has nil Bloom filter")
	}

	for _, rec := range records {
		if !filter.MayContain([]byte(rec.Key)) {
			t.Fatalf(
				"Bloom filter returned false negative for key %q",
				rec.Key,
			)
		}
	}
}

func TestBloomFilteredLookupAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	records := []record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
		{
			Key: "banana",
			Seq: 2,
			Entry: record.Entry{
				Value: []byte("yellow"),
			},
		},
	}

	opts := DefaultWriteOptions()
	opts.Bloom.Enabled = true
	opts.Bloom.FalsePositiveRate = 0.01

	_, err := CreateFromRecordsWithOptions(records, path, opts)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions: %v", err)
	}

	reopened := &SSTable{
		Path: path,
	}

	got, err := reopened.Open("banana")
	if err != nil {
		t.Fatalf("Open banana: %v", err)
	}

	if string(got) != "yellow" {
		t.Fatalf("Open banana = %q, want %q", got, "yellow")
	}
}

func TestBloomFilteredLookupMissingKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	records := []record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
		{
			Key: "banana",
			Seq: 2,
			Entry: record.Entry{
				Value: []byte("yellow"),
			},
		},
	}

	opts := DefaultWriteOptions()
	opts.Bloom.Enabled = true
	opts.Bloom.FalsePositiveRate = 0.01

	table, err := CreateFromRecordsWithOptions(records, path, opts)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions: %v", err)
	}

	_, err = table.Open("grape")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Open missing key error = %v, want ErrNotFound", err)
	}
}

func TestBloomFilterIncludesTombstones(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	records := []record.Record{
		{
			Key: "deleted-key",
			Seq: 1,
			Entry: record.Entry{
				Deleted: true,
			},
		},
	}

	opts := DefaultWriteOptions()
	opts.Bloom.Enabled = true
	opts.Bloom.FalsePositiveRate = 0.01

	_, err := CreateFromRecordsWithOptions(records, path, opts)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions: %v", err)
	}

	reopened := &SSTable{
		Path: path,
	}

	if err := reopened.loadMetadata(); err != nil {
		t.Fatalf("loadMetadata: %v", err)
	}

	reopened.metaMu.RLock()
	filter := reopened.filter
	reopened.metaMu.RUnlock()

	if filter == nil {
		t.Fatal("reopened SSTable has nil Bloom filter")
	}

	if !filter.MayContain([]byte("deleted-key")) {
		t.Fatal("Bloom filter returned false negative for tombstone")
	}

	_, err = reopened.Open("deleted-key")
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Open tombstone error = %v, want ErrDeleted", err)
	}
}

func TestBloomFilterCanBeDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	records := []record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
	}

	opts := DefaultWriteOptions()
	opts.Bloom.Enabled = false

	_, err := CreateFromRecordsWithOptions(records, path, opts)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions: %v", err)
	}

	reopened := &SSTable{
		Path: path,
	}

	if err := reopened.loadMetadata(); err != nil {
		t.Fatalf("loadMetadata: %v", err)
	}

	reopened.metaMu.RLock()
	filter := reopened.filter
	reopened.metaMu.RUnlock()

	if filter != nil {
		t.Fatal("Bloom filter is non-nil when Bloom filtering is disabled")
	}

	got, err := reopened.Open("apple")
	if err != nil {
		t.Fatalf("Open apple: %v", err)
	}

	if string(got) != "red" {
		t.Fatalf("Open apple = %q, want red", got)
	}
}

func TestEmptySSTableHasNoBloomFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	opts := DefaultWriteOptions()
	opts.Bloom.Enabled = true
	opts.Bloom.FalsePositiveRate = 0.01

	_, err := CreateFromRecordsWithOptions(nil, path, opts)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions: %v", err)
	}

	reopened := &SSTable{
		Path: path,
	}

	if err := reopened.loadMetadata(); err != nil {
		t.Fatalf("loadMetadata: %v", err)
	}

	reopened.metaMu.RLock()
	filter := reopened.filter
	reopened.metaMu.RUnlock()

	if filter != nil {
		t.Fatal("empty SSTable unexpectedly has Bloom filter")
	}

	_, err = reopened.Open("anything")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Open empty table error = %v, want ErrNotFound", err)
	}
}

func TestReadFooterRejectsInvalidFilterOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	createTestSSTableWithBloom(t, path)

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	footerOffset := info.Size() - footerSize

	var encoded [8]byte
	binary.LittleEndian.PutUint64(
		encoded[:],
		uint64(info.Size()+100),
	)

	if _, err := file.WriteAt(
		encoded[:],
		footerOffset+16,
	); err != nil {
		t.Fatalf("WriteAt filter offset: %v", err)
	}

	reopened := &SSTable{
		Path: path,
	}

	err = reopened.loadMetadata()
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf(
			"loadMetadata error = %v, want ErrCorruptSSTable",
			err,
		)
	}
}

func TestReadFooterRejectsInvalidFilterLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	createTestSSTableWithBloom(t, path)

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	footerOffset := info.Size() - footerSize

	var encoded [8]byte
	binary.LittleEndian.PutUint64(
		encoded[:],
		uint64(info.Size()),
	)

	if _, err := file.WriteAt(
		encoded[:],
		footerOffset+24,
	); err != nil {
		t.Fatalf("WriteAt filter length: %v", err)
	}

	reopened := &SSTable{
		Path: path,
	}

	err = reopened.loadMetadata()
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf(
			"loadMetadata error = %v, want ErrCorruptSSTable",
			err,
		)
	}
}

func TestLoadMetadataRejectsCorruptBloomFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	createTestSSTableWithBloom(t, path)

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()

	footer, err := readFooter(file)
	if err != nil {
		t.Fatalf("readFooter: %v", err)
	}

	if footer.filterLength == 0 {
		t.Fatal("expected nonempty Bloom filter section")
	}

	// The first Bloom codec byte is the Bloom format version.
	if _, err := file.WriteAt(
		[]byte{0xff},
		int64(footer.filterOffset),
	); err != nil {
		t.Fatalf("WriteAt Bloom version: %v", err)
	}

	reopened := &SSTable{
		Path: path,
	}

	err = reopened.loadMetadata()
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf(
			"loadMetadata error = %v, want ErrCorruptSSTable",
			err,
		)
	}
}
