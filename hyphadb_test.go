package hyphadb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testOptions(dataDir string) Options {
	return Options{
		DataDir:    dataDir,
		Memtable:   MemtableOptions{MaxEntries: 100},
		Compaction: CompactionOptions{TableCountThreshold: 100},
	}
}

func TestOpenExclusivelyLocksDatabaseDirectory(t *testing.T) {
	dataDir := t.TempDir()
	first, err := Open(testOptions(dataDir))
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}

	if _, err := Open(testOptions(dataDir)); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("second Open error = %v, want ErrDatabaseLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	reopened, err := Open(testOptions(dataDir))
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("reopened Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "LOCK")); err != nil {
		t.Fatalf("persistent lock file: %v", err)
	}
}

func TestFailedOpenReleasesDatabaseLock(t *testing.T) {
	dataDir := t.TempDir()
	manifestPath := filepath.Join(dataDir, "MANIFEST")
	if err := os.WriteFile(manifestPath, []byte("corrupt"), 0600); err != nil {
		t.Fatalf("write corrupt manifest: %v", err)
	}
	if _, err := Open(testOptions(dataDir)); err == nil {
		t.Fatal("Open succeeded with corrupt manifest")
	}
	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("remove corrupt manifest: %v", err)
	}

	database, err := Open(testOptions(dataDir))
	if err != nil {
		t.Fatalf("Open after failed Open: %v", err)
	}
	defer database.Close()
}

func TestPublicAPIEnforcesConfiguredInputLimits(t *testing.T) {
	opts := testOptions(t.TempDir())
	opts.Limits = LimitsOptions{
		MaxKeyBytes:        4,
		MaxValueBytes:      5,
		MaxBatchOperations: 2,
		MaxBatchBytes:      10,
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer database.Close()

	if err := database.Put("12345", []byte("value")); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversized key error = %v, want ErrKeyTooLarge", err)
	}
	if err := database.Put("key", []byte("123456")); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("oversized value error = %v, want ErrValueTooLarge", err)
	}
	if _, err := database.Get("12345"); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversized Get key error = %v, want ErrKeyTooLarge", err)
	}
	if err := database.Delete("12345"); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversized Delete key error = %v, want ErrKeyTooLarge", err)
	}
	if _, err := database.NewIterator(IteratorOptions{
		Start: "12345",
	}); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversized iterator bound error = %v, want ErrKeyTooLarge", err)
	}

	batch := database.NewBatch()
	if err := batch.Put("one", []byte("1")); err != nil {
		t.Fatalf("first batch Put: %v", err)
	}
	if err := batch.Delete("two"); err != nil {
		t.Fatalf("batch Delete: %v", err)
	}
	if err := batch.Put("tri", []byte("3")); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("operation limit error = %v, want ErrBatchTooLarge", err)
	}
	if err := batch.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	byteLimited := database.NewBatch()
	if err := byteLimited.Put("four", []byte("12345")); err != nil {
		t.Fatalf("byte-limited first Put: %v", err)
	}
	if err := byteLimited.Delete("xy"); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("byte limit error = %v, want ErrBatchTooLarge", err)
	}
}

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
