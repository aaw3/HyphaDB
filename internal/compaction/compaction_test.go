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
			got, err := merged.Open(tt.key)
			if err != nil {
				t.Fatalf("Open(%q): %v", tt.key, err)
			}

			if string(got) != tt.want {
				t.Fatalf(
					"Open(%q) = %q, want %q",
					tt.key,
					got,
					tt.want,
				)
			}
		})
	}
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

	got, err := merged.Open("apple")
	if err != nil {
		t.Fatalf("Open(a): %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("Open(a) = %q, want red", got)
	}

	_, err = merged.Open("banana")
	if !errors.Is(err, sstable.ErrNotFound) {
		t.Fatalf("Open(b) error = %v, want %v",
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
