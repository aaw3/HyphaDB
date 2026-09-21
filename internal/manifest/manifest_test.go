package manifest

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
				SizeBytes:   128,
				SmallestKey: "apple",
				LargestKey:  "banana",
			},
			{
				ID:          5,
				Path:        "data-5.sst",
				Level:       2,
				SizeBytes:   512,
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

func TestWriteUsesVersionedBinaryFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	if err := Write(path, &Manifest{}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) < manifestHeaderSize {
		t.Fatalf("manifest size = %d, want at least %d", len(data), manifestHeaderSize)
	}
	if got := string(data[:8]); got != string(manifestMagic[:]) {
		t.Fatalf("magic = %q, want %q", got, manifestMagic)
	}
	if got := binary.LittleEndian.Uint32(data[8:12]); got != manifestVersion {
		t.Fatalf("version = %d, want %d", got, manifestVersion)
	}
}

func TestReadRejectsChecksumCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	if err := Write(path, &Manifest{NextSSTableID: 3}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = Read(path)
	if !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("Read error = %v, want ErrCorruptManifest", err)
	}
}

func TestReadRejectsTruncatedAndTrailingData(t *testing.T) {
	encoded, err := encode(&Manifest{NextWALSegmentID: 4})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	for name, data := range map[string][]byte{
		"truncated": encoded[:len(encoded)-1],
		"trailing":  append(append([]byte(nil), encoded...), 0xff),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "MANIFEST")
			if err := os.WriteFile(path, data, 0644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			_, err := Read(path)
			if !errors.Is(err, ErrCorruptManifest) {
				t.Fatalf("Read error = %v, want ErrCorruptManifest", err)
			}
		})
	}
}

func TestReadRejectsUnsupportedVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	data, err := encode(&Manifest{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	binary.LittleEndian.PutUint32(data[8:12], manifestVersion+1)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = Read(path)
	if !errors.Is(err, ErrUnsupportedManifestVersion) {
		t.Fatalf("Read error = %v, want ErrUnsupportedManifestVersion", err)
	}
}

func TestReadRejectsImpossibleSSTableCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	data, err := encode(&Manifest{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	countOffset := manifestHeaderSize + 8 + 8
	binary.LittleEndian.PutUint32(data[countOffset:countOffset+4], maxSSTableCount+1)
	binary.LittleEndian.PutUint32(
		data[16:20],
		manifestChecksum(data[:16], data[manifestHeaderSize:]),
	)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = Read(path)
	if !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("Read error = %v, want ErrCorruptManifest", err)
	}
}

func TestReadRejectsOversizedMetadataLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	data, err := encode(&Manifest{SSTables: []SSTableMetadata{{
		ID:   1,
		Path: "data-1.sst",
	}}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	pathLengthOffset := manifestHeaderSize + manifestPayloadHeaderSize + 8 + 4 + 8
	binary.LittleEndian.PutUint32(
		data[pathLengthOffset:pathLengthOffset+4],
		maxMetadataStringSize+1,
	)
	binary.LittleEndian.PutUint32(
		data[16:20],
		manifestChecksum(data[:16], data[manifestHeaderSize:]),
	)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err = Read(path)
	if !errors.Is(err, ErrCorruptManifest) {
		t.Fatalf("Read error = %v, want ErrCorruptManifest", err)
	}
}

func TestWriteRejectsOversizedMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "MANIFEST")
	err := Write(path, &Manifest{SSTables: []SSTableMetadata{{
		ID:   1,
		Path: strings.Repeat("x", maxMetadataStringSize+1),
	}}})
	if err == nil {
		t.Fatal("Write succeeded with oversized metadata")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("manifest should not have been created: %v", statErr)
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
