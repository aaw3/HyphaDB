package sstable

import (
	"bytes"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/compression"
	"github.com/aaw3/hyphadb/internal/record"
)

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

func TestCompressedSSTableIterator(t *testing.T) {
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
		{
			Key: "dragonfruit",
			Seq: 3,
			Entry: record.Entry{
				Value: bytes.Repeat([]byte("pink"), 1000),
			},
		},
	}

	opts := DefaultWriteOptions()
	sst, err := CreateFromRecordsWithOptions(
		records,
		path,
		WriteOptions{
			BlockSize:                 2048,
			Compression:               compression.LZ4,
			MinCompressionSavingsRate: opts.MinCompressionSavingsRate,
		},
	)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions failed: %v", err)
	}

	it, err := sst.Iterator()
	if err != nil {
		t.Fatalf("Iterator error: %v", err)
	}
	defer it.Close()

	var got []string
	for it.Next() {
		got = append(got, it.Record().Key)
	}

	if err := it.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	want := []string{"apple", "banana", "dragonfruit"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
}

func TestIteratorSeekStartsAtLowerBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seek.sst")
	records := []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
		{Key: "carrot", Seq: 3, Entry: record.Entry{Value: []byte("orange")}},
		{Key: "date", Seq: 4, Entry: record.Entry{Value: []byte("brown")}},
		{Key: "fig", Seq: 5, Entry: record.Entry{Value: []byte("purple")}},
	}

	sst, err := CreateFromRecords(records, path, 32)
	if err != nil {
		t.Fatalf("CreateFromRecords failed: %v", err)
	}

	it, err := sst.Iterator()
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	defer it.Close()

	if err := it.Seek("coconut"); err != nil {
		t.Fatalf("Seek error: %v", err)
	}

	var got []string
	for it.Next() {
		got = append(got, it.Record().Key)
	}

	if err := it.Err(); err != nil {
		t.Fatalf("Iterator returned error: %v", err)
	}

	want := []string{"date", "fig"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
}

func TestIteratorSeekPastLastKeyReturnsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seek-past-last.sst")
	records := []record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
	}

	sst, err := CreateFromRecords(records, path, 32)
	if err != nil {
		t.Fatalf("CreateFromRecords failed: %v", err)
	}

	it, err := sst.Iterator()
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}
	defer it.Close()

	if err := it.Seek("zebra"); err != nil {
		t.Fatalf("Seek error: %v", err)
	}

	if it.Next() {
		t.Fatalf("Next returned key %q, want false", it.Record().Key)
	}

	if err := it.Err(); err != nil {
		t.Fatalf("Iterator returned error: %v", err)
	}
}

func TestCompressedSSTableMaxSeq(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compressed.sst")

	records := []record.Record{
		{
			Key: "apple",
			Seq: 5,
			Entry: record.Entry{
				Value: bytes.Repeat([]byte("red"), 1000),
			},
		},
		{
			Key: "banana",
			Seq: 54,
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
			BlockSize:                 2048,
			Compression:               compression.LZ4,
			MinCompressionSavingsRate: opts.MinCompressionSavingsRate,
		},
	)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions failed: %v", err)
	}

	got, err := sst.MaxSeq()
	if err != nil {
		t.Fatalf("MaxSeq error: %v", err)
	}

	if got != 54 {
		t.Fatalf("MaxSeq = %d, want 54", got)
	}
}
