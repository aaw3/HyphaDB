package db

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/manifest"
	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
)

func TestIteratorMergesSourcesAndAppliesRange(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	tablePath := "data-0.sst"
	_, err = sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red-old")}},
		{Key: "carrot", Seq: 3, Entry: record.Entry{Value: []byte("orange")}},
		{Key: "fig", Seq: 8, Entry: record.Entry{Value: []byte("purple")}},
	}, tablePath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords: %v", err)
	}

	imm := memtable.New()
	imm.Put(record.Record{
		Key: "apple",
		Seq: 5,
		Entry: record.Entry{
			Value: []byte("red-imm"),
		},
	})
	imm.Put(record.Record{
		Key: "banana",
		Seq: 4,
		Entry: record.Entry{
			Value: []byte("yellow"),
		},
	})
	imm.Put(record.Record{
		Key: "fig",
		Seq: 9,
		Entry: record.Entry{
			Deleted: true,
		},
	})

	database.immutableMemtables = append(
		database.immutableMemtables,
		&memtable.ImmutableMemTable{
			MemTable: imm,
		},
	)
	database.sstables = append(
		database.sstables,
		database.newSSTable(manifest.SSTableMetadata{
			ID:   0,
			Path: tablePath,
		}),
	)

	database.memtable.Put(record.Record{
		Key: "apple",
		Seq: 7,
		Entry: record.Entry{
			Value: []byte("red-active"),
		},
	})
	database.memtable.Put(record.Record{
		Key: "date",
		Seq: 6,
		Entry: record.Entry{
			Value: []byte("brown"),
		},
	})

	it, err := database.NewIterator(IteratorOptions{
		Start: "banana",
		End:   "fig",
	})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	got := collectIteratorKeyValues(t, it)
	want := []string{
		"banana=yellow",
		"carrot=orange",
		"date=brown",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
}

func TestIteratorRangeBoundsCanBeUnbounded(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	for i, key := range []string{"apple", "banana", "carrot", "date"} {
		database.memtable.Put(record.Record{
			Key: key,
			Seq: uint64(i + 1),
			Entry: record.Entry{
				Value: []byte(key),
			},
		})
	}

	tests := []struct {
		name string
		opts IteratorOptions
		want []string
	}{
		{
			name: "start only",
			opts: IteratorOptions{Start: "carrot"},
			want: []string{"carrot=carrot", "date=date"},
		},
		{
			name: "end only",
			opts: IteratorOptions{End: "carrot"},
			want: []string{"apple=apple", "banana=banana"},
		},
		{
			name: "empty bounds",
			opts: IteratorOptions{},
			want: []string{
				"apple=apple",
				"banana=banana",
				"carrot=carrot",
				"date=date",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it, err := database.NewIterator(tt.opts)
			if err != nil {
				t.Fatalf("NewIterator: %v", err)
			}
			defer it.Close()

			got := collectIteratorKeyValues(t, it)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("records = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIteratorRangeSupportsPrefixScanBounds(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	for i, key := range []string{
		"user",
		"users\x00alice",
		"users\x00bob",
		"users\x01",
		"users\x01carol",
	} {
		database.memtable.Put(record.Record{
			Key: key,
			Seq: uint64(i + 1),
			Entry: record.Entry{
				Value: []byte(key),
			},
		})
	}

	it, err := database.NewIterator(IteratorOptions{
		Start: "users\x00",
		End:   "users\x01",
	})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	got := collectIteratorKeyValues(t, it)
	want := []string{
		"users\x00alice=users\x00alice",
		"users\x00bob=users\x00bob",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
}

func TestScanPrefixUsesRangeBounds(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	for i, key := range []string{
		"user",
		"users\x00alice",
		"users\x00bob",
		"users\x01",
		"users\x01carol",
	} {
		database.memtable.Put(record.Record{
			Key: key,
			Seq: uint64(i + 1),
			Entry: record.Entry{
				Value: []byte(key),
			},
		})
	}

	it, err := database.ScanPrefix("users\x00")
	if err != nil {
		t.Fatalf("ScanPrefix: %v", err)
	}
	defer it.Close()

	got := collectIteratorKeyValues(t, it)
	want := []string{
		"users\x00alice=users\x00alice",
		"users\x00bob=users\x00bob",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
}

func TestPrefixEnd(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{
			name:   "empty",
			prefix: "",
			want:   "",
		},
		{
			name:   "ascii",
			prefix: "users\x00",
			want:   "users\x01",
		},
		{
			name:   "carry trailing max byte",
			prefix: "users\x00\xff",
			want:   "users\x01",
		},
		{
			name:   "no finite end",
			prefix: "\xff",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PrefixEnd(tt.prefix); got != tt.want {
				t.Fatalf("PrefixEnd(%q) = %q, want %q", tt.prefix, got, tt.want)
			}
		})
	}
}

func TestIteratorSuppressesHighestSequenceTombstone(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	database.memtable.Put(record.Record{
		Key: "apple",
		Seq: 2,
		Entry: record.Entry{
			Deleted: true,
		},
	})

	imm := memtable.New()
	imm.Put(record.Record{
		Key: "apple",
		Seq: 1,
		Entry: record.Entry{
			Value: []byte("red"),
		},
	})
	database.immutableMemtables = append(
		database.immutableMemtables,
		&memtable.ImmutableMemTable{
			MemTable: imm,
		},
	)

	it, err := database.NewIterator(IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	got := collectIteratorKeyValues(t, it)
	if len(got) != 0 {
		t.Fatalf("records = %v, want empty", got)
	}
}

func TestIteratorCollapsesDuplicateKeysToHighestSequence(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	tablePath := "data-0.sst"
	_, err = sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red-old")}},
	}, tablePath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords: %v", err)
	}

	imm := memtable.New()
	imm.Put(record.Record{
		Key: "apple",
		Seq: 5,
		Entry: record.Entry{
			Value: []byte("red-imm"),
		},
	})

	database.immutableMemtables = append(
		database.immutableMemtables,
		&memtable.ImmutableMemTable{
			MemTable: imm,
		},
	)
	database.sstables = append(
		database.sstables,
		database.newSSTable(manifest.SSTableMetadata{
			ID:   0,
			Path: tablePath,
		}),
	)
	database.memtable.Put(record.Record{
		Key: "apple",
		Seq: 9,
		Entry: record.Entry{
			Value: []byte("red-active"),
		},
	})

	it, err := database.NewIterator(IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	got := collectIteratorKeyValues(t, it)
	want := []string{"apple=red-active"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
}

func TestIteratorUsesSequenceOverSourceRecency(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	tablePath := "data-0.sst"
	_, err = sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 9, Entry: record.Entry{Value: []byte("red-sstable")}},
	}, tablePath, sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords: %v", err)
	}

	database.sstables = append(
		database.sstables,
		database.newSSTable(manifest.SSTableMetadata{
			ID:   0,
			Path: tablePath,
		}),
	)
	database.memtable.Put(record.Record{
		Key: "apple",
		Seq: 5,
		Entry: record.Entry{
			Value: []byte("red-active"),
		},
	})

	it, err := database.NewIterator(IteratorOptions{})
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	got := collectIteratorKeyValues(t, it)
	want := []string{"apple=red-sstable"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("records = %v, want %v", got, want)
	}
}

func TestIteratorReturnsErrClosed(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = database.NewIterator(IteratorOptions{})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("NewIterator error = %v, want %v", err, ErrClosed)
	}
}

// ----------
//
//	Helpers
//
// ----------
func collectIteratorKeyValues(t *testing.T, it *Iterator) []string {
	t.Helper()

	var got []string

	for it.Next() {
		rec := it.Record()
		got = append(got, rec.Key+"="+string(rec.Value))
	}

	if err := it.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	return got
}
