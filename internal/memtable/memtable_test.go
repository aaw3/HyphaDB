package memtable

import (
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
)

func TestMemTablePutAndGet(t *testing.T) {
	mt := New()

	want := record.Record{
		Key: "apple",
		Seq: 1,
		Entry: record.Entry{
			Value: []byte("red"),
		},
	}

	mt.Put(want)

	got, ok := mt.Get("apple")
	if !ok {
		t.Fatal("expected key apple")
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("record = %+v, want %+v", got, want)
	}
}

func TestMemTableOverwriteReplacesRecord(t *testing.T) {
	mt := New()

	mt.Put(record.Record{
		Key: "apple",
		Seq: 1,
		Entry: record.Entry{
			Value: []byte("old"),
		},
	})

	mt.Put(record.Record{
		Key: "apple",
		Seq: 2,
		Entry: record.Entry{
			Value: []byte("new"),
		},
	})

	got, ok := mt.Get("apple")
	if !ok {
		t.Fatal("expected key apple")
	}

	if got.Seq != 2 {
		t.Fatalf("sequence = %d, want 2", got.Seq)
	}

	if string(got.Value) != "new" {
		t.Fatalf("value = %q, want new", got.Value)
	}

	if len(mt.Records()) != 1 {
		t.Fatalf(
			"record count = %d, want 1",
			len(mt.Records()),
		)
	}
}

func TestMemTablePreservesTombstone(t *testing.T) {
	mt := New()

	mt.Put(record.Record{
		Key: "apple",
		Seq: 3,
		Entry: record.Entry{
			Deleted: true,
		},
	})

	got, ok := mt.Get("apple")
	if !ok {
		t.Fatal("expected tombstone for key apple")
	}

	if !got.Deleted {
		t.Fatal("record is not marked deleted")
	}

	if got.Seq != 3 {
		t.Fatalf("sequence = %d, want 3", got.Seq)
	}
}

func TestMemTableRecordsAreSorted(t *testing.T) {
	mt := New()

	mt.Put(record.Record{Key: "date", Seq: 4})
	mt.Put(record.Record{Key: "apple", Seq: 1})
	mt.Put(record.Record{Key: "cherry", Seq: 3})
	mt.Put(record.Record{Key: "banana", Seq: 2})

	records := mt.Records()

	got := make([]string, 0, len(records))
	for _, rec := range records {
		got = append(got, rec.Key)
	}

	want := []string{
		"apple",
		"banana",
		"cherry",
		"date",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
}
