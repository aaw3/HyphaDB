package sstable

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
)

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
