package db

import (
	"os"
	"testing"
)

func TestFlushDeletesWALAndRestartReadsFromSSTable(t *testing.T) {
	tempDir := t.TempDir()
	oldDir, err := os.Getwd()

	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(tempDir); err != nil {
		t.Fatal(err)
	}

	database, err := New(2, 10)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	if err := database.Put("a", []byte("apple")); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if err := database.Put("b", []byte("banana")); err != nil {
		t.Fatalf("put b: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// wal gets flushed to sstable
	if _, err := os.Stat("wal-0.log"); !os.IsNotExist(err) {
		t.Fatalf("wal-0.log should be deleted after flush, got err=%v", err)
	}

	// wal-1.log replaces wal-0.log after flush
	if _, err := os.Stat("wal-1.log"); err != nil {
		t.Fatalf("expected active wal-1.log to exist: %v", err)
	}

	// sstable created after flush
	if _, err := os.Stat("data-0.sst"); err != nil {
		t.Fatalf("expected data-0.sst to exist: %v", err)
	}

	// reopeen the database and check saved values
	reopened, err := New(2, 10)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.Get("a")
	if err != nil {
		t.Fatalf("get a after restart: %v", err)
	}
	if string(got) != "apple" {
		t.Fatalf("get a = %q, want apple", got)
	}

	got, err = reopened.Get("b")
	if err != nil {
		t.Fatalf("get b after restart: %v", err)
	}
	if string(got) != "banana" {
		t.Fatalf("get b = %q, want banana", got)
	}
}
