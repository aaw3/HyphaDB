package sstable

import (
	"errors"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
)

func TestCreateFromRecordAndOpen(t *testing.T) {
	path := t.TempDir() + "/test_sstable.sst"
	records := []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
		{Key: "carrot", Seq: 3, Entry: record.Entry{Value: []byte("orange")}},
	}

	sst, err := CreateFromRecords(records, path, DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords failed: %v", err)
	}

	got, err := sst.Open("banana")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if string(got) != "yellow" {
		t.Fatalf("Open returned wrong value: got %q, want %q", got, "yellow")
	}
}

func TestOpenMissingKeyReturnsErrNotFound(t *testing.T) {
	path := t.TempDir() + "/test_sstable_missing_key.sst"
	records := []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
	}

	sst, err := CreateFromRecords(records, path, DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords failed: %v", err)
	}

	for _, key := range []string{"carrot", "date", "eggplant"} {
		_, err := sst.Open(key)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Open for missing key %q returned wrong error: got %v, want %v", key, err, ErrNotFound)
		}
	}
}

func TestOpenDeleteKeyReturnsErrDeleted(t *testing.T) {
	path := t.TempDir() + "/test_sstable_deleted_key.sst"
	records := []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
		{Key: "carrot", Seq: 3, Entry: record.Entry{Deleted: true}},
	}

	sst, err := CreateFromRecords(records, path, DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords failed: %v", err)
	}

	_, err = sst.Open("carrot")
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Open for deleted key returned wrong error: got %v, want %v", err, ErrDeleted)
	}
}

func TestCreateFromRecordsWithTinyBlockSize(t *testing.T) {
	path := t.TempDir() + "/test_sstable_tiny_block.sst"
	records := []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
		{Key: "carrot", Seq: 3, Entry: record.Entry{Value: []byte("orange")}},
		{Key: "dragonfruit", Seq: 4, Entry: record.Entry{Value: []byte("pink")}},
	}

	sst, err := CreateFromRecords(records, path, 32)
	if err != nil {
		t.Fatalf("CreateFromRecords failed: %v", err)
	}

	if len(sst.index) < 2 {
		t.Fatalf("Expected multiple blocks for tiny block size, got %d", len(sst.index))
	}

	got, err := sst.Open("dragonfruit")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if string(got) != "pink" {
		t.Fatalf("Open returned wrong value: got %q, want %q", got, "pink")
	}
}

func TestIteratorReturnsRecordsInOrder(t *testing.T) {
	path := t.TempDir() + "/test_sstable_iterator.sst"
	records := []record.Record{

		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
		{Key: "carrot", Seq: 3, Entry: record.Entry{Value: []byte("orange")}},
	}

	sst, err := CreateFromRecords(records, path, DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords failed: %v", err)
	}

	it, err := sst.Iterator()
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	defer it.Close()

	var got []string
	for it.Next() {
		rec := it.Record()
		got = append(got, rec.Key)
	}

	if err := it.Err(); err != nil {
		t.Fatalf("Iterator returned error: %v", err)
	}

	want := []string{"apple", "banana", "carrot"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Iterator returned wrong keys: got %v, want %v", got, want)
	}
}

func TestMaxSeq(t *testing.T) {
	path := t.TempDir() + "/test_sstable_max_seq.sst"
	records := []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 5, Entry: record.Entry{Value: []byte("yellow")}},
		{Key: "carrot", Seq: 3, Entry: record.Entry{Value: []byte("orange")}},
	}

	sst, err := CreateFromRecords(records, path, 32)
	if err != nil {
		t.Fatalf("CreateFromRecords failed: %v", err)
	}

	got, err := sst.MaxSeq()
	if err != nil {
		t.Fatalf("MaxSeq failed: %v", err)
	}

	if got != 5 {
		t.Fatalf("MaxSeq returned wrong value: got %d, want %d", got, 5)
	}
}

func TestReadFooterRejectsUnsupportedVersion(t *testing.T) {
	path := t.TempDir() + "/test_sstable_unsupported_version.sst"

	_, err := CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	}, path, DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords error: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile error: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}

	versionOffset := info.Size() - int64(footerSize) + 22

	if _, err := file.Seek(versionOffset, io.SeekStart); err != nil {
		t.Fatalf("Seek error: %v", err)
	}

	if _, err := file.Write([]byte{99}); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	reopened := &SSTable{Path: path}

	_, err = reopened.Open("apple")
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrCorruptSSTable,
		)
	}
}

func TestReadFooterRejectsNonzeroReservedByte(t *testing.T) {
	path := t.TempDir() + "/test_sstable_nonzero_reserved_byte.sst"

	_, err := CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	}, path, DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords error: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile error: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}

	reservedOffset := info.Size() - 1

	if _, err := file.Seek(reservedOffset, io.SeekStart); err != nil {
		t.Fatalf("Seek error: %v", err)
	}

	if _, err := file.Write([]byte{1}); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	reopened := &SSTable{Path: path}

	_, err = reopened.Open("apple")
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrCorruptSSTable,
		)
	}
}
