package memtable

import (
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
)

func TestSnapshotIteratorIsUnaffectedByLaterWrites(t *testing.T) {
	mt := New()
	value := []byte("red")
	mt.Put(record.Record{
		Key: "apple",
		Seq: 1,
		Entry: record.Entry{
			Value: value,
		},
	})

	it := mt.SnapshotIterator()
	mt.Put(record.Record{
		Key: "banana",
		Seq: 2,
		Entry: record.Entry{
			Value: []byte("yellow"),
		},
	})
	value[0] = 'X'

	var got []string
	for it.Next() {
		got = append(got, it.Record().Key+"="+string(it.Record().Value))
	}

	want := []string{"apple=red"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot records = %v, want %v", got, want)
	}
}

func TestSnapshotIteratorSeek(t *testing.T) {
	mt := New()
	for i, key := range []string{"apple", "banana", "carrot"} {
		mt.Put(record.Record{Key: key, Seq: uint64(i + 1)})
	}

	it := mt.SnapshotIterator()
	if err := it.Seek("banana"); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if !it.Next() || it.Record().Key != "banana" {
		t.Fatalf("record after Seek = %+v, want banana", it.Record())
	}
}
