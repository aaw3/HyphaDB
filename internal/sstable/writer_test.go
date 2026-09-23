package sstable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
)

func TestCreateWritesRestartPoints(t *testing.T) {
	records := make([]record.Record, 10)
	for i := range records {
		records[i] = record.Record{
			Key:   fmt.Sprintf("key/%04d", i),
			Seq:   uint64(i + 1),
			Entry: record.Entry{Value: []byte("value")},
		}
	}

	opts := DefaultWriteOptions()
	opts.RestartInterval = 4
	sst, err := CreateFromRecordsWithOptions(
		records,
		filepath.Join(t.TempDir(), "data-0.sst"),
		opts,
	)
	if err != nil {
		t.Fatalf("CreateFromRecordsWithOptions: %v", err)
	}

	logical, err := sst.readLogicalBlock(sst.index[0])
	if err != nil {
		t.Fatalf("readLogicalBlock: %v", err)
	}
	cursor, err := newLogicalBlockCursorForVersion(logical, sst.formatVersion)
	if err != nil {
		t.Fatalf("newLogicalBlockCursorForVersion: %v", err)
	}
	if cursor.restartInterval != 4 {
		t.Fatalf("restart interval = %d, want 4", cursor.restartInterval)
	}
	if got, want := len(cursor.restartOffsets)/4, 3; got != want {
		t.Fatalf("restart count = %d, want %d", got, want)
	}
}

func TestCreateFailureDoesNotPublishPartialSSTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data-0.sst")
	_, err := CreateFromRecords([]record.Record{
		{Key: "banana", Seq: 1},
		{Key: "apple", Seq: 2},
	}, path, DefaultBlockSize)
	if !errors.Is(err, ErrUnsortedRecords) {
		t.Fatalf("CreateFromRecords error = %v, want ErrUnsortedRecords", err)
	}

	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("partial final SSTable was published: %v", statErr)
	}
	if _, statErr := os.Stat(path + ".tmp"); !os.IsNotExist(statErr) {
		t.Fatalf("temporary SSTable was not removed: %v", statErr)
	}
}
