package db

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/manifest"
	"github.com/aaw3/hyphadb/internal/record"
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

func TestMultipleSnapshotsUseIndependentSequences(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put red: %v", err)
	}
	oldSnapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot old: %v", err)
	}
	defer oldSnapshot.Close()

	if err := database.Put("apple", []byte("green")); err != nil {
		t.Fatalf("Put green: %v", err)
	}
	newSnapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot new: %v", err)
	}
	defer newSnapshot.Close()

	if err := database.Put("apple", []byte("blue")); err != nil {
		t.Fatalf("Put blue: %v", err)
	}

	got, err := oldSnapshot.Get("apple")
	if err != nil {
		t.Fatalf("old snapshot Get: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("old snapshot Get = %q, want red", got)
	}
	got, err = newSnapshot.Get("apple")
	if err != nil {
		t.Fatalf("new snapshot Get: %v", err)
	}
	if string(got) != "green" {
		t.Fatalf("new snapshot Get = %q, want green", got)
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

func TestCompactionPreservesActiveSnapshotHistory(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	oldPath := "data-0.sst"
	_, err = sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 5, Entry: record.Entry{Value: []byte("red")}},
	}, oldPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create old SSTable: %v", err)
	}
	database.sstables = append(database.sstables, database.newSSTable(manifest.SSTableMetadata{
		ID: 0, Path: oldPath,
	}))
	database.manifest.SSTables = append(database.manifest.SSTables, manifest.SSTableMetadata{
		ID: 0, Path: oldPath,
	})
	database.manifest.NextSSTableID = 1
	database.nextSeq = 6

	snapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	defer snapshot.Close()

	newPath := "data-1.sst"
	_, err = sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 10, Entry: record.Entry{Value: []byte("green")}},
	}, newPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create new SSTable: %v", err)
	}
	database.sstables = append(database.sstables, database.newSSTable(manifest.SSTableMetadata{
		ID: 1, Path: newPath,
	}))
	database.manifest.SSTables = append(database.manifest.SSTables, manifest.SSTableMetadata{
		ID: 1, Path: newPath,
	})
	database.manifest.NextSSTableID = 2
	database.nextSeq = 11

	if err := database.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
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

	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old SSTable still exists, stat error = %v", err)
	}
}

func TestCompactionUsesOldestReader(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	addTable := func(id uint64, sequence uint64, value string) {
		t.Helper()
		path := fmt.Sprintf("data-%d.sst", id)
		_, err := sstable.CreateFromRecords([]record.Record{{
			Key: "apple", Seq: sequence,
			Entry: record.Entry{Value: []byte(value)},
		}}, path, sstable.DefaultBlockSize)
		if err != nil {
			t.Fatalf("create SSTable %d: %v", id, err)
		}
		database.sstables = append(database.sstables, database.newSSTable(manifest.SSTableMetadata{
			ID: id, Path: path,
		}))
		database.manifest.SSTables = append(database.manifest.SSTables, manifest.SSTableMetadata{
			ID: id, Path: path,
		})
		database.manifest.NextSSTableID = id + 1
		database.nextSeq = sequence + 1
	}

	addTable(0, 1, "red")
	oldSnapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot old: %v", err)
	}

	addTable(1, 2, "green")
	newSnapshot, err := database.NewSnapshot()
	if err != nil {
		t.Fatalf("NewSnapshot new: %v", err)
	}
	addTable(2, 3, "blue")

	if err := database.Compact(); err != nil {
		t.Fatalf("Compact with two snapshots: %v", err)
	}
	if got := sstableSequences(t, database.sstables[0]); !reflect.DeepEqual(got, []uint64{3, 2, 1}) {
		t.Fatalf("versions with two snapshots = %v, want [3 2 1]", got)
	}

	if err := oldSnapshot.Close(); err != nil {
		t.Fatalf("Close old snapshot: %v", err)
	}
	if got, want := database.oldestReaderLocked(); got != newSnapshot.Sequence() || !want {
		t.Fatalf("oldest reader after close = %d, %v; want %d, true", got, want, newSnapshot.Sequence())
	}
	if err := newSnapshot.Close(); err != nil {
		t.Fatalf("Close new snapshot: %v", err)
	}
}

func sstableSequences(t *testing.T, table *sstable.SSTable) []uint64 {
	t.Helper()
	it, err := table.Iterator()
	if err != nil {
		t.Fatalf("SSTable Iterator: %v", err)
	}
	defer it.Close()

	var sequences []uint64
	for it.Next() {
		sequences = append(sequences, it.Record().Seq)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("SSTable iterator error: %v", err)
	}
	return sequences
}
