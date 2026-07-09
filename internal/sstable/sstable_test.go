package sstable

import (
	"errors"
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
