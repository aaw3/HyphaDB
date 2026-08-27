package compaction

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
)

func TestMergeSSTablesKeepsNewestValue(t *testing.T) {
	dir := t.TempDir()

	oldPath := filepath.Join(dir, "old.sst")
	newPath := filepath.Join(dir, "new.sst")
	mergedPath := filepath.Join(dir, "merged.sst")

	oldTable, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
	}, oldPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create old SSTable failed: %v", err)
	}

	newTable, err := sstable.CreateFromRecords([]record.Record{
		{
			Key: "banana",
			Seq: 3,
			Entry: record.Entry{
				Value: []byte("#ffe135"), // override old value
			},
		},
		{
			Key: "cherry",
			Seq: 4,
			Entry: record.Entry{
				Value: []byte("#de3163"),
			},
		},
	}, newPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create new SSTable failed: %v", err)
	}

	// merge two tables, insert oldest then newest
	merged, err := MergeSSTables(
		[]*sstable.SSTable{oldTable, newTable},
		mergedPath,
	)
	if err != nil {
		t.Fatalf("MergeSSTables failed: %v", err)
	}

	tests := []struct {
		key  string
		want string
	}{
		{key: "apple", want: "red"},
		{key: "banana", want: "#ffe135"},
		{key: "cherry", want: "#de3163"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got, err := merged.Get(tt.key)
			if err != nil {
				t.Fatalf("Get(%q): %v", tt.key, err)
			}

			if string(got) != tt.want {
				t.Fatalf(
					"Get(%q) = %q, want %q",
					tt.key,
					got,
					tt.want,
				)
			}
		})
	}
}

func TestMergeSSTablesUsesHighestSequence(t *testing.T) {
	dir := t.TempDir()

	olderPath := filepath.Join(dir, "older.sst")
	newerPath := filepath.Join(dir, "newer.sst")
	mergedPath := filepath.Join(dir, "merged.sst")

	older, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 10, Entry: record.Entry{Value: []byte("green")}},
	}, olderPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create older SSTable failed: %v", err)
	}
	newer, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 5, Entry: record.Entry{Value: []byte("red")}},
	}, newerPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create newer SSTable failed: %v", err)
	}

	merged, err := MergeSSTables([]*sstable.SSTable{older, newer}, mergedPath)
	if err != nil {
		t.Fatalf("MergeSSTables failed: %v", err)
	}

	got, err := merged.Get("apple")
	if err != nil {
		t.Fatalf("Get(apple): %v", err)
	}
	if string(got) != "green" {
		t.Fatalf("Get(apple) = %q, want green", got)
	}
}

func TestMergeSSTablesPreservesVersionsForReader(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old.sst")
	newPath := filepath.Join(dir, "new.sst")
	mergedPath := filepath.Join(dir, "merged.sst")

	old, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 5, Entry: record.Entry{Value: []byte("red")}},
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("old")}},
	}, oldPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create old SSTable failed: %v", err)
	}
	newer, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 10, Entry: record.Entry{Value: []byte("green")}},
	}, newPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create new SSTable failed: %v", err)
	}

	merged, err := MergeSSTablesWithRetention(
		[]*sstable.SSTable{old, newer},
		mergedPath,
		uint64Ptr(7),
	)
	if err != nil {
		t.Fatalf("MergeSSTablesWithRetention failed: %v", err)
	}

	got, ok, err := merged.GetRecordAt("apple", 7)
	if err != nil {
		t.Fatalf("GetRecordAt snapshot: %v", err)
	}
	if !ok || got.Seq != 5 || string(got.Value) != "red" {
		t.Fatalf("snapshot record = %+v, %v; want seq=5 value=red", got, ok)
	}

	got, ok, err = merged.GetRecordAt("apple", 10)
	if err != nil {
		t.Fatalf("GetRecordAt latest: %v", err)
	}
	if !ok || got.Seq != 10 || string(got.Value) != "green" {
		t.Fatalf("latest record = %+v, %v; want seq=10 value=green", got, ok)
	}
}

func TestMergeSSTablesPreservesTombstoneForReader(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old.sst")
	newPath := filepath.Join(dir, "new.sst")
	mergedPath := filepath.Join(dir, "merged.sst")

	old, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 5, Entry: record.Entry{Value: []byte("red")}},
	}, oldPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create old SSTable failed: %v", err)
	}
	newer, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 10, Entry: record.Entry{Deleted: true}},
	}, newPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create new SSTable failed: %v", err)
	}

	merged, err := MergeSSTablesWithRetention(
		[]*sstable.SSTable{old, newer},
		mergedPath,
		uint64Ptr(7),
	)
	if err != nil {
		t.Fatalf("MergeSSTablesWithRetention failed: %v", err)
	}

	got, ok, err := merged.GetRecordAt("apple", 7)
	if err != nil {
		t.Fatalf("GetRecordAt snapshot: %v", err)
	}
	if !ok || got.Seq != 5 || string(got.Value) != "red" {
		t.Fatalf("snapshot record = %+v, %v; want seq=5 value=red", got, ok)
	}

	got, ok, err = merged.GetRecordAt("apple", 10)
	if err != nil {
		t.Fatalf("GetRecordAt latest: %v", err)
	}
	if !ok || got.Seq != 10 || !got.Deleted {
		t.Fatalf("latest record = %+v, %v; want seq=10 tombstone", got, ok)
	}
}

func uint64Ptr(value uint64) *uint64 {
	return &value
}

func TestMergeSSTablesDropsDeletedKey(t *testing.T) {
	dir := t.TempDir()

	oldPath := filepath.Join(dir, "old.sst")
	newPath := filepath.Join(dir, "new.sst")
	mergedPath := filepath.Join(dir, "merged.sst")

	oldTable, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "banana", Seq: 2, Entry: record.Entry{Value: []byte("yellow")}},
	}, oldPath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create old SSTable failed: %v", err)
	}

	newTable, err := sstable.CreateFromRecords([]record.Record{
		{Key: "banana", Seq: 3, Entry: record.Entry{Deleted: true}},
	}, newPath, sstable.DefaultBlockSize)

	if err != nil {
		t.Fatalf("create new SSTable failed: %v", err)
	}

	merged, err := MergeSSTables(
		[]*sstable.SSTable{oldTable, newTable},
		mergedPath,
	)

	if err != nil {
		t.Fatalf("MergeSSTables failed: %v", err)
	}

	got, err := merged.Get("apple")
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("Get(a) = %q, want red", got)
	}

	_, err = merged.Get("banana")
	if !errors.Is(err, sstable.ErrNotFound) {
		t.Fatalf("Get(b) error = %v, want %v",
			err,
			sstable.ErrNotFound,
		)
	}
}

func TestMergeSSTablesProducesSortedOutput(t *testing.T) {
	dir := t.TempDir()

	firstPath := filepath.Join(dir, "first.sst")
	secondPath := filepath.Join(dir, "second.sst")
	mergedPath := filepath.Join(dir, "merged.sst")

	// A tiny block size forces the source SSTables to contain multiple blocks.
	first, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
		{Key: "cherry", Seq: 2, Entry: record.Entry{Value: []byte("red")}},
		{Key: "elderberry", Seq: 3, Entry: record.Entry{Value: []byte("purple")}},
	}, firstPath, 32)

	if err != nil {
		t.Fatalf("create first SSTable: %v", err)
	}

	second, err := sstable.CreateFromRecords([]record.Record{
		{Key: "banana", Seq: 4, Entry: record.Entry{Value: []byte("yellow")}},
		{Key: "date", Seq: 5, Entry: record.Entry{Value: []byte("brown")}},
		{Key: "fig", Seq: 6, Entry: record.Entry{Value: []byte("purple")}},
	}, secondPath, 32)

	if err != nil {
		t.Fatalf("create second SSTable: %v", err)
	}

	_, err = MergeSSTables(
		[]*sstable.SSTable{first, second},
		mergedPath,
	)
	if err != nil {
		t.Fatalf("MergeSSTables: %v", err)
	}

	// Reopen from path to ensure the output can be parsed from disk.
	reopened := &sstable.SSTable{Path: mergedPath}

	it, err := reopened.Iterator()
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

	want := []string{"apple", "banana", "cherry", "date", "elderberry", "fig"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
}
