package sstable

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
)

func TestCreateFromRecordAndGet(t *testing.T) {
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

	got, err := sst.Get("banana")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(got) != "yellow" {
		t.Fatalf("Get returned wrong value: got %q, want %q", got, "yellow")
	}
}

func TestGetMissingKeyReturnsErrNotFound(t *testing.T) {
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
		_, err := sst.Get(key)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get for missing key %q returned wrong error: got %v, want %v", key, err, ErrNotFound)
		}
	}
}

func TestGetDeleteKeyReturnsErrDeleted(t *testing.T) {
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

	_, err = sst.Get("carrot")
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get for deleted key returned wrong error: got %v, want %v", err, ErrDeleted)
	}
}

func TestGetRecordAtReturnsVisibleVersion(t *testing.T) {
	path := t.TempDir() + "/versions.sst"
	records := []record.Record{
		{Key: "apple", Seq: 10, Entry: record.Entry{Value: []byte("green")}},
		{Key: "apple", Seq: 5, Entry: record.Entry{Value: []byte("red")}},
	}

	sst, err := CreateFromRecords(records, path, DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords failed: %v", err)
	}

	got, ok, err := sst.GetRecordAt("apple", 7)
	if err != nil {
		t.Fatalf("GetRecordAt failed: %v", err)
	}
	if !ok || got.Seq != 5 || string(got.Value) != "red" {
		t.Fatalf("GetRecordAt(apple, 7) = %+v, %v; want seq=5 value=red", got, ok)
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

	got, err := sst.Get("dragonfruit")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(got) != "pink" {
		t.Fatalf("Get returned wrong value: got %q, want %q", got, "pink")
	}
}

func TestConcurrentMissingKeyReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.sst")

	sst, err := CreateFromRecords(
		[]record.Record{
			{
				Key: "banana",
				Seq: 1,
				Entry: record.Entry{
					Value: []byte("yellow"),
				},
			},
		},
		path,
		DefaultBlockSize,
	)
	if err != nil {
		t.Fatalf("CreateFromRecords error: %v", err)
	}

	for _, key := range []string{"a", "z"} {

		t.Run(key, func(t *testing.T) {
			var wg sync.WaitGroup

			for i := 0; i < 16; i++ {
				wg.Add(1)

				go func() {
					defer wg.Done()

					for j := 0; j < 100; j++ {
						_, err := sst.Get(key)
						if !errors.Is(err, ErrNotFound) {
							t.Errorf(
								"Get(%q) error = %v, want %v",
								key,
								err,
								ErrNotFound,
							)
							return
						}
					}
				}()
			}

			wg.Wait()
		})
	}
}
