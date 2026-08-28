package manifest

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestWriteReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")

	want := &Manifest{
		NextSSTableID:    7,
		NextWALSegmentID: 4,
		SSTables: []SSTableMetadata{
			{
				ID:          1,
				Path:        "data-1.sst",
				Level:       0,
				SmallestKey: "apple",
				LargestKey:  "banana",
			},
			{
				ID:          5,
				Path:        "data-5.sst",
				Level:       2,
				SmallestKey: "carrot",
				LargestKey:  "zucchini",
			},
		},
	}

	if err := Write(path, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("manifest = %+v, want %+v", got, want)
	}
}

func TestReadMissingManifestReturnsDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if got.NextSSTableID != 0 {
		t.Fatalf(
			"NextSSTableID = %d, want 0",
			got.NextSSTableID,
		)
	}

	if got.NextWALSegmentID != 0 {
		t.Fatalf(
			"NextWALSegmentID = %d, want 0",
			got.NextWALSegmentID,
		)
	}

	if len(got.SSTables) != 0 {
		t.Fatalf(
			"SSTables = %v, want empty",
			got.SSTables,
		)
	}
}

func TestReadAdvancesUnsafeNextSSTableID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")

	stored := &Manifest{
		NextSSTableID:    2,
		NextWALSegmentID: 0,
		SSTables: []SSTableMetadata{
			{
				ID:   1,
				Path: "data-1.sst",
			},
			{
				ID:   8,
				Path: "compact-8.sst",
			},
			{
				ID:   4,
				Path: "data-4.sst",
			},
		},
	}

	if err := Write(path, stored); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if got.NextSSTableID != 9 {
		t.Fatalf(
			"NextSSTableID = %d, want 9",
			got.NextSSTableID,
		)
	}
}

func TestReadPreservesAlreadySafeNextSSTableID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")

	stored := &Manifest{
		NextSSTableID:    20,
		NextWALSegmentID: 3,
		SSTables: []SSTableMetadata{
			{
				ID:   1,
				Path: "data-1.sst",
			},
			{
				ID:   8,
				Path: "compact-8.sst",
			},
		},
	}

	if err := Write(path, stored); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	got, err := Read(path)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if got.NextSSTableID != 20 {
		t.Fatalf(
			"NextSSTableID = %d, want 20",
			got.NextSSTableID,
		)
	}
}
