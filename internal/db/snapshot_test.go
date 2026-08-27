package db

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/sstable"
)

func TestSnapshotReadsPointInTime(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put red: %v", err)
	}
	snapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snapshot.Close()

	if got, want := snapshot.Sequence(), uint64(1); got != want {
		t.Fatalf("Sequence = %d, want %d", got, want)
	}
	if err := database.Put("apple", []byte("green")); err != nil {
		t.Fatalf("Put green: %v", err)
	}

	got, err := snapshot.Get("apple")
	if err != nil {
		t.Fatalf("snapshot Get: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("snapshot Get = %q, want red", got)
	}

	got, err = database.Get("apple")
	if err != nil {
		t.Fatalf("latest Get: %v", err)
	}
	if string(got) != "green" {
		t.Fatalf("latest Get = %q, want green", got)
	}
}

func TestSnapshotTombstoneVisibility(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	snapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snapshot.Close()

	if err := database.Delete("apple"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err := snapshot.Get("apple")
	if err != nil {
		t.Fatalf("snapshot Get: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("snapshot Get = %q, want red", got)
	}

	if _, err := database.Get("apple"); !errors.Is(err, sstable.ErrNotFound) {
		t.Fatalf("latest Get error = %v, want ErrNotFound", err)
	}
}

func TestSnapshotIteratorUsesCapturedSequence(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put apple: %v", err)
	}
	if err := database.Put("banana", []byte("yellow")); err != nil {
		t.Fatalf("Put banana: %v", err)
	}
	snapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}

	if err := database.Put("carrot", []byte("orange")); err != nil {
		t.Fatalf("Put carrot: %v", err)
	}
	it, err := snapshot.NewIterator(IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("Close snapshot: %v", err)
	}

	got := collectIteratorKeyValues(t, it)
	if err := it.Close(); err != nil {
		t.Fatalf("Close iterator: %v", err)
	}
	want := []string{"apple=red", "banana=yellow"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot records = %v, want %v", got, want)
	}
}

func TestSnapshotAndIteratorRegisterIndependently(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	snapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	sequence := snapshot.Sequence()
	if got := database.activeReaders[sequence]; got != 1 {
		t.Fatalf("active readers after snapshot = %d, want 1", got)
	}

	it, err := snapshot.NewIterator(IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	if got := database.activeReaders[sequence]; got != 2 {
		t.Fatalf("active readers after iterator = %d, want 2", got)
	}

	if err := snapshot.Close(); err != nil {
		t.Fatalf("Close snapshot: %v", err)
	}
	if got := database.activeReaders[sequence]; got != 1 {
		t.Fatalf("active readers after snapshot close = %d, want 1", got)
	}

	if err := it.Close(); err != nil {
		t.Fatalf("Close iterator: %v", err)
	}
	if _, ok := database.activeReaders[sequence]; ok {
		t.Fatalf("sequence %d remains registered after iterator close", sequence)
	}
}

func TestClosedSnapshotRejectsReadsAndIterators(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	snapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("Close snapshot: %v", err)
	}

	if _, err := snapshot.Get("apple"); !errors.Is(err, ErrSnapshotClosed) {
		t.Fatalf("closed snapshot Get error = %v, want ErrSnapshotClosed", err)
	}
	if _, err := snapshot.NewIterator(IteratorOptions{}); !errors.Is(err, ErrSnapshotClosed) {
		t.Fatalf("closed snapshot NewIterator error = %v, want ErrSnapshotClosed", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
