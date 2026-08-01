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
