package wal

import (
	"errors"
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

func TestReplayStatsIncludeUnappliedAndEmptyBatchIDs(t *testing.T) {
	useTempWorkingDirectory(t)

	w, err := NewSegment(5)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	if err := w.WriteRecord(record.Record{
		BatchID: 40, BatchKind: record.BatchBegin,
	}); err != nil {
		t.Fatalf("write incomplete batch begin: %v", err)
	}
	if err := w.WriteRecord(record.Record{
		Key: "incomplete", Seq: 41, Entry: record.Entry{Value: []byte("value")},
		BatchID: 40, BatchKind: record.BatchOperation,
	}); err != nil {
		t.Fatalf("write incomplete batch operation: %v", err)
	}
	if err := w.WriteBatch(50, nil, true); err != nil {
		t.Fatalf("write empty batch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	mt := memtable.New()
	stats, err := ReplayIntoWithStats(SegmentPath(5), mt)
	if err != nil {
		t.Fatalf("ReplayIntoWithStats: %v", err)
	}
	if stats.MaxSequence != 50 {
		t.Fatalf("MaxSequence = %d, want 50", stats.MaxSequence)
	}
	if _, ok := mt.Get("incomplete"); ok {
		t.Fatal("incomplete batch was applied")
	}
}

func TestReplayRepairsTruncatedFinalFrame(t *testing.T) {
	useTempWorkingDirectory(t)

	w, err := NewSegment(6)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	if err := w.Write("apple", 1, []byte("red")); err != nil {
		t.Fatalf("write apple: %v", err)
	}
	info, err := os.Stat(SegmentPath(6))
	if err != nil {
		t.Fatalf("stat after first record: %v", err)
	}
	validSize := info.Size()
	if err := w.Write("banana", 2, []byte("yellow")); err != nil {
		t.Fatalf("write banana: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err = os.Stat(SegmentPath(6))
	if err != nil {
		t.Fatalf("stat complete WAL: %v", err)
	}
	if err := os.Truncate(SegmentPath(6), info.Size()-3); err != nil {
		t.Fatalf("truncate final frame: %v", err)
	}

	mt := memtable.New()
	if err := ReplayInto(SegmentPath(6), mt); err != nil {
		t.Fatalf("ReplayInto truncated WAL: %v", err)
	}
	if _, ok := mt.Get("apple"); !ok {
		t.Fatal("complete record before truncated tail was not replayed")
	}
	if _, ok := mt.Get("banana"); ok {
		t.Fatal("truncated record was replayed")
	}
	info, err = os.Stat(SegmentPath(6))
	if err != nil {
		t.Fatalf("stat repaired WAL: %v", err)
	}
	if info.Size() != validSize {
		t.Fatalf("repaired WAL size = %d, want %d", info.Size(), validSize)
	}

	w, err = NewSegment(6)
	if err != nil {
		t.Fatalf("reopen repaired WAL: %v", err)
	}
	if err := w.Write("cherry", 3, []byte("dark-red")); err != nil {
		t.Fatalf("write after repair: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close repaired WAL: %v", err)
	}

	replayed := memtable.New()
	if err := ReplayInto(SegmentPath(6), replayed); err != nil {
		t.Fatalf("replay after append: %v", err)
	}
	for _, key := range []string{"apple", "cherry"} {
		if _, ok := replayed.Get(key); !ok {
			t.Fatalf("missing replayed key %q", key)
		}
	}
}

func TestReplayRejectsCorruptFrame(t *testing.T) {
	useTempWorkingDirectory(t)

	w, err := NewSegment(7)
	if err != nil {
		t.Fatalf("NewSegment: %v", err)
	}
	if err := w.Write("apple", 1, []byte("red")); err != nil {
		t.Fatalf("write apple: %v", err)
	}
	if err := w.Write("banana", 2, []byte("yellow")); err != nil {
		t.Fatalf("write banana: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(SegmentPath(7))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[walHeaderSize+frameHeaderSize] ^= 0xff
	if err := os.WriteFile(SegmentPath(7), data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	err = ReplayInto(SegmentPath(7), memtable.New())
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("ReplayInto error = %v, want ErrCorruptWAL", err)
	}
}

func TestNewSegmentRejectsUnknownWALFormat(t *testing.T) {
	useTempWorkingDirectory(t)

	if err := os.WriteFile(SegmentPath(8), []byte("legacy-or-corrupt"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := NewSegment(8)
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("NewSegment error = %v, want ErrCorruptWAL", err)
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
