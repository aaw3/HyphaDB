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
		SSTablePaths: []string{
			"data-1.sst",
			"data-5.sst",
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

	if len(got.SSTablePaths) != 0 {
		t.Fatalf(
			"SSTablePaths = %v, want empty",
			got.SSTablePaths,
		)
	}
}

func TestReadAdvancesUnsafeNextSSTableID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")

	stored := &Manifest{
		NextSSTableID:    2,
		NextWALSegmentID: 0,
		SSTablePaths: []string{
			"data-1.sst",
			"compact-8.sst",
			"data-4.sst",
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
		SSTablePaths: []string{
			"data-1.sst",
			"compact-8.sst",
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
