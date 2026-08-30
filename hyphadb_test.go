package hyphadb

import (
	"errors"
	"testing"
)

func TestPublicAPIStoresAndRecoversValues(t *testing.T) {
	dataDir := t.TempDir()
	database, err := Open(Options{
		DataDir: dataDir,
		Memtable: MemtableOptions{
			MaxEntries: 2,
		},
		Compaction: CompactionOptions{
			TableCountThreshold: 10,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	input := []byte("red")
	if err := database.Put("apple", input); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := database.Put("banana", []byte("yellow")); err != nil {
		t.Fatalf("Put banana: %v", err)
	}
	input[0] = 'x'

	got, err := database.Get("apple")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("apple = %q, want red", got)
	}
	got[0] = 'x'

	got, err = database.Get("apple")
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("stored apple = %q, want red", got)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{
		DataDir: dataDir,
		Memtable: MemtableOptions{
			MaxEntries: 2,
		},
		Compaction: CompactionOptions{
			TableCountThreshold: 10,
		},
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got, err = reopened.Get("apple")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("recovered apple = %q, want red", got)
	}
}

func TestPublicAPISnapshotAndIterator(t *testing.T) {
	database, err := Open(Options{
		DataDir:    t.TempDir(),
		Memtable:   MemtableOptions{MaxEntries: 100},
		Compaction: CompactionOptions{TableCountThreshold: 100},
		BlockCache: BlockCacheOptions{CapacityBytes: 1024},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put apple: %v", err)
	}
	snapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snapshot.Close()

	if err := database.Put("apple", []byte("green")); err != nil {
		t.Fatalf("update apple: %v", err)
	}
	got, err := snapshot.Get("apple")
	if err != nil {
		t.Fatalf("snapshot Get: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("snapshot apple = %q, want red", got)
	}

	iterator, err := database.NewIterator(IteratorOptions{Start: "apple", End: "zebra"})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer iterator.Close()
	if !iterator.Next() {
		t.Fatalf("iterator returned no records, err=%v", iterator.Err())
	}
	if iterator.Key() != "apple" || string(iterator.Value()) != "green" {
		t.Fatalf("iterator record = %q=%q, want apple=green", iterator.Key(), iterator.Value())
	}
	if iterator.Next() {
		t.Fatal("iterator returned unexpected second record")
	}
	if err := iterator.Err(); err != nil {
		t.Fatalf("iterator Err: %v", err)
	}

	if _, err := database.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing Get error = %v, want ErrNotFound", err)
	}
}

func TestPublicAPIBatchCommitAndCancel(t *testing.T) {
	database, err := Open(Options{
		DataDir:    t.TempDir(),
		Memtable:   MemtableOptions{MaxEntries: 100},
		Compaction: CompactionOptions{TableCountThreshold: 100},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	batch := database.NewBatch()
	if err := batch.Put("document/1", []byte("body")); err != nil {
		t.Fatalf("batch Put: %v", err)
	}
	if err := batch.Put("index/body/1", []byte("document/1")); err != nil {
		t.Fatalf("batch index Put: %v", err)
	}
	if err := batch.Commit(WriteOptions{Sync: true}); err != nil {
		t.Fatalf("batch Commit: %v", err)
	}

	for _, key := range []string{"document/1", "index/body/1"} {
		if _, err := database.Get(key); err != nil {
			t.Fatalf("Get %q after batch: %v", key, err)
		}
	}

	canceled := database.NewBatch()
	if err := canceled.Put("not-visible", []byte("value")); err != nil {
		t.Fatalf("canceled Put: %v", err)
	}
	if err := canceled.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := database.Get("not-visible"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canceled value error = %v, want ErrNotFound", err)
	}
}
