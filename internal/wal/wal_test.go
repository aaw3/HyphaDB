package wal

import (
	"os"
	"reflect"
	"testing"

	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
)

func useTempWorkingDirectory(t *testing.T) {
	t.Helper()

	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd error: %v", err)
	}

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("Chdir temp directory failed: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Chdir(oldDir); err != nil {
			t.Errorf("restore working directory failed: %v", err)
		}
	})
}

func TestWALWriteAndReplay(t *testing.T) {
	useTempWorkingDirectory(t)

	w, err := NewSegment(0)
	if err != nil {
		t.Fatalf("NewSegment error: %v", err)
	}

	records := []record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
		{
			Key: "banana",
			Seq: 2,
			Entry: record.Entry{
				Value: []byte("yellow"),
			},
		},
	}

	for _, rec := range records {
		if err := w.WriteRecord(rec); err != nil {
			t.Fatalf("WriteRecord(%q): %v", rec.Key, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	mt := memtable.New()
	if err := ReplayInto(SegmentPath(0), mt); err != nil {
		t.Fatalf("ReplayInto error: %v", err)
	}

	for _, want := range records {
		got, ok := mt.Get(want.Key)
		if !ok {
			t.Fatalf("missing replayed key %q", want.Key)
		}

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("record = %+v, want %+v", got, want)
		}
	}
}

func TestWALReplayPreservesTombstone(t *testing.T) {
	useTempWorkingDirectory(t)

	w, err := NewSegment(1)
	if err != nil {
		t.Fatalf("NewSegment error: %v", err)
	}

	want := record.Record{
		Key: "deleted-key",
		Seq: 9,
		Entry: record.Entry{
			Deleted: true,
		},
	}

	if err := w.WriteRecord(want); err != nil {
		t.Fatalf("WriteRecord error: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	mt := memtable.New()
	if err := ReplayInto(SegmentPath(1), mt); err != nil {
		t.Fatalf("ReplayInto error: %v", err)
	}

	got, ok := mt.Get("deleted-key")
	if !ok {
		t.Fatal("expected replayed tombstone")
	}

	if !got.Deleted {
		t.Fatal("replayed record is not marked deleted")
	}

	if got.Seq != want.Seq {
		t.Fatalf("sequence = %d, want %d", got.Seq, want.Seq)
	}
}

func TestWALBatchReplayRequiresCommitMarker(t *testing.T) {
	useTempWorkingDirectory(t)

	w, err := NewSegment(4)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	if err := w.WriteRecord(record.Record{BatchID: 7, BatchKind: record.BatchBegin}); err != nil {
		t.Fatalf("write batch begin: %v", err)
	}
	if err := w.WriteRecord(record.Record{
		Key:     "incomplete",
		Seq:     1,
		Entry:   record.Entry{Value: []byte("value")},
		BatchID: 7, BatchKind: record.BatchOperation,
	}); err != nil {
		t.Fatalf("write batch operation: %v", err)
	}
	if err := w.WriteBatch(8, []record.Record{{
		Key:   "complete",
		Seq:   2,
		Entry: record.Entry{Value: []byte("value")},
	}}, true); err != nil {
		t.Fatalf("write complete batch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mt := memtable.New()
	if err := ReplayInto(SegmentPath(4), mt); err != nil {
		t.Fatalf("ReplayInto: %v", err)
	}
	if _, ok := mt.Get("incomplete"); ok {
		t.Fatal("incomplete batch was replayed")
	}
	if _, ok := mt.Get("complete"); !ok {
		t.Fatal("complete batch was not replayed")
	}
}

func TestListSegmentsReturnsNumericOrder(t *testing.T) {
	useTempWorkingDirectory(t)

	for _, id := range []uint64{10, 2, 1} {
		w, err := NewSegment(id)
		if err != nil {
			t.Fatalf("NewSegment(%d): %v", id, err)
		}

		if err := w.Close(); err != nil {
			t.Fatalf("Close segment %d: %v", id, err)
		}
	}

	segments, err := ListSegments()
	if err != nil {
		t.Fatalf("ListSegments error: %v", err)
	}

	got := make([]uint64, 0, len(segments))
	for _, segment := range segments {
		got = append(got, segment.ID)
	}

	want := []uint64{1, 2, 10}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("segment IDs = %v, want %v", got, want)
	}
}

func TestRemoveSegment(t *testing.T) {
	useTempWorkingDirectory(t)

	w, err := NewSegment(3)
	if err != nil {
		t.Fatalf("NewSegment error: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := RemoveSegment(3); err != nil {
		t.Fatalf("RemoveSegment error: %v", err)
	}

	if _, err := os.Stat(SegmentPath(3)); !os.IsNotExist(err) {
		t.Fatalf("segment still exists or unexpected error: %v", err)
	}

	// Removing an already-missing segment should be idempotent.
	if err := RemoveSegment(3); err != nil {
		t.Fatalf("RemoveSegment missing file: %v", err)
	}
}

func TestReplayMissingSegmentSucceeds(t *testing.T) {
	useTempWorkingDirectory(t)

	mt := memtable.New()

	if err := ReplayInto("wal-999.log", mt); err != nil {
		t.Fatalf("ReplayInto missing file: %v", err)
	}
}
