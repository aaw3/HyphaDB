package skiplist

import (
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
)

func TestSkipListGetReturnsInsertedRecords(t *testing.T) {
	sl := New()

	records := []record.Record{
		{Key: "b", Seq: 1, Entry: record.Entry{Value: []byte("banana")}},
		{Key: "a", Seq: 2, Entry: record.Entry{Value: []byte("apple")}},
		{Key: "c", Seq: 3, Entry: record.Entry{Value: []byte("cherry")}},
	}

	// Insert into new skip list
	for _, rec := range records {
		sl.Put(rec)
	}

	// verify that we can retrieve the records
	for _, want := range records {
		got, ok := sl.Get(want.Key)
		if !ok {
			t.Fatalf("expected key %q", want.Key)
		}
		// check if returned record was mismatched
		if got.Key != want.Key || got.Seq != want.Seq || string(got.Value) != string(want.Value) || got.Deleted != want.Deleted {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	}

	if _, ok := sl.Get("missing"); ok {
		t.Fatal("did not expect missing key")
	}
}

func TestSkipListOverwriteReplacesExistingRecord(t *testing.T) {
	sl := New()

	sl.Put(record.Record{Key: "a", Seq: 1, Entry: record.Entry{Value: []byte("old")}})
	sl.Put(record.Record{Key: "a", Seq: 2, Entry: record.Entry{Value: []byte("new")}})

	got, ok := sl.Get("a")
	if !ok {
		t.Fatal("expected key a")
	}
	if got.Seq != 2 || string(got.Value) != "new" {
		t.Fatalf("got %+v, want seq=2 value=new", got)
	}

	records := sl.Records()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
}

func TestSkipListStoresTombstoneRecord(t *testing.T) {
	sl := New()

	sl.Put(record.Record{Key: "a", Seq: 1, Entry: record.Entry{Value: []byte("old")}})
	sl.Put(record.Record{Key: "a", Seq: 2, Entry: record.Entry{Deleted: true}})

	got, ok := sl.Get("a")
	if !ok {
		t.Fatal("expected tombstone record")
	}
	if !got.Deleted || got.Seq != 2 {
		t.Fatalf("got %+v, want deleted record with seq=2", got)
	}
}

func TestSkipListRecordsReturnsSortedRecords(t *testing.T) {
	sl := New()

	sl.Put(record.Record{Key: "delta", Seq: 4})
	sl.Put(record.Record{Key: "alpha", Seq: 1})
	sl.Put(record.Record{Key: "charlie", Seq: 3})
	sl.Put(record.Record{Key: "bravo", Seq: 2})

	records := sl.Records()

	got := make([]string, 0, len(records))
	for _, rec := range records {
		got = append(got, rec.Key)
	}

	want := []string{"alpha", "bravo", "charlie", "delta"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSkipListRecordsEmpty(t *testing.T) {
	sl := New()

	records := sl.Records()
	if len(records) != 0 {
		t.Fatalf("got %d records, want 0", len(records))
	}
}

func TestSkipListWithSeedReturnsSortedRecords(t *testing.T) {
	sl := NewWithMaxLevelAndSeed(8, 0)

	sl.Put(record.Record{Key: "c"})
	sl.Put(record.Record{Key: "a"})
	sl.Put(record.Record{Key: "b"})

	records := sl.Records()

	got := []string{records[0].Key, records[1].Key, records[2].Key}
	want := []string{"a", "b", "c"}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
