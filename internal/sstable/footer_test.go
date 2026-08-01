package sstable

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aaw3/hyphadb/internal/record"
)

func TestReadFooterRejectsUnsupportedVersion(t *testing.T) {
	path := t.TempDir() + "/test_sstable_unsupported_version.sst"

	_, err := CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	}, path, DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords error: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile error: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}

	versionOffset := info.Size() - int64(footerSize) + 38

	if _, err := file.Seek(versionOffset, io.SeekStart); err != nil {
		t.Fatalf("Seek error: %v", err)
	}

	if _, err := file.Write([]byte{99}); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	reopened := &SSTable{Path: path}

	_, err = reopened.Get("apple")
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrCorruptSSTable,
		)
	}
}

func TestReadFooterRejectsNonzeroReservedByte(t *testing.T) {
	path := t.TempDir() + "/test_sstable_nonzero_reserved_byte.sst"

	_, err := CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	}, path, DefaultBlockSize)
	if err != nil {
		t.Fatalf("CreateFromRecords error: %v", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile error: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}

	reservedOffset := info.Size() - 1

	if _, err := file.Seek(reservedOffset, io.SeekStart); err != nil {
		t.Fatalf("Seek error: %v", err)
	}

	if _, err := file.Write([]byte{1}); err != nil {
		t.Fatalf("Write error: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	reopened := &SSTable{Path: path}

	_, err = reopened.Get("apple")
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("error = %v, want %v",
			err,
			ErrCorruptSSTable,
		)
	}
}

func TestFooterRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Simulated data section:
	// bytes [0, 100)   blocks
	// bytes [100, 120) index
	// bytes [120, 130) filter
	if _, err := file.Write(make([]byte, 130)); err != nil {
		file.Close()
		t.Fatalf("write data: %v", err)
	}

	want := footerMetadata{
		indexOffset:  100,
		indexLength:  20,
		filterOffset: 120,
		filterLength: 10,
	}

	if err := writeFooter(
		file,
		want.indexOffset,
		want.indexLength,
		want.filterOffset,
		want.filterLength,
	); err != nil {
		file.Close()
		t.Fatalf("writeFooter: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	file, err = os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()

	got, err := readFooter(file)
	if err != nil {
		t.Fatalf("readFooter: %v", err)
	}

	if got != want {
		t.Fatalf("footer = %+v, want %+v", got, want)
	}
}

func TestFooterRoundTripWithoutFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table.sst")

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.Write(make([]byte, 120)); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := writeFooter(file, 100, 20, 0, 0); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	file, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	got, err := readFooter(file)
	if err != nil {
		t.Fatalf("readFooter: %v", err)
	}

	if got.indexOffset != 100 ||
		got.indexLength != 20 ||
		got.filterOffset != 0 ||
		got.filterLength != 0 {
		t.Fatalf("unexpected footer: %+v", got)
	}
}

func TestReadFooterRejectsSmallFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "small.sst")

	if err := os.WriteFile(path, make([]byte, footerSize-1), 0o600); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	_, err = readFooter(file)
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf("error = %v, want %v", err, ErrCorruptSSTable)
	}
}

func TestReadFooterRejectsInvalidMagic(t *testing.T) {
	path := createValidFooterFile(t, 100, 20, 0, 0)

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatal(err)
	}

	magicOffset := info.Size() - int64(footerSize) + 32

	if _, err := file.WriteAt([]byte("XXXXXX"), magicOffset); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	assertCorruptFooter(t, path)
}

func TestReadFooterRejectsNonzeroFlags(t *testing.T) {
	path := createValidFooterFile(t, 100, 20, 0, 0)

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatal(err)
	}

	flagsOffset := info.Size() - int64(footerSize) + 39

	if _, err := file.WriteAt([]byte{1}, flagsOffset); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	assertCorruptFooter(t, path)
}

func TestReadFooterRejectsIndexOffsetBeyondDataEnd(t *testing.T) {
	path := createRawFooterFile(t, 100, footerMetadata{
		indexOffset:  101,
		indexLength:  0,
		filterOffset: 0,
		filterLength: 0,
	})

	assertCorruptFooter(t, path)
}

func TestReadFooterRejectsIndexLengthBeyondDataEnd(t *testing.T) {
	path := createRawFooterFile(t, 100, footerMetadata{
		indexOffset:  90,
		indexLength:  11,
		filterOffset: 0,
		filterLength: 0,
	})

	assertCorruptFooter(t, path)
}

func TestReadFooterRejectsZeroFilterLengthWithOffset(t *testing.T) {
	path := createRawFooterFile(t, 100, footerMetadata{
		indexOffset:  80,
		indexLength:  20,
		filterOffset: 100,
		filterLength: 0,
	})

	assertCorruptFooter(t, path)
}

func TestReadFooterRejectsGapAfterIndexWithoutFilter(t *testing.T) {
	path := createRawFooterFile(t, 100, footerMetadata{
		indexOffset:  80,
		indexLength:  10,
		filterOffset: 0,
		filterLength: 0,
	})

	assertCorruptFooter(t, path)
}

func TestReadFooterRejectsFilterOffsetNotAfterIndex(t *testing.T) {
	path := createRawFooterFile(t, 100, footerMetadata{
		indexOffset:  60,
		indexLength:  20,
		filterOffset: 81,
		filterLength: 19,
	})

	assertCorruptFooter(t, path)
}

func TestReadFooterRejectsFilterLengthBeyondDataEnd(t *testing.T) {
	path := createRawFooterFile(t, 100, footerMetadata{
		indexOffset:  60,
		indexLength:  20,
		filterOffset: 80,
		filterLength: 21,
	})

	assertCorruptFooter(t, path)
}

func TestReadFooterRejectsGapAfterFilter(t *testing.T) {
	path := createRawFooterFile(t, 100, footerMetadata{
		indexOffset:  60,
		indexLength:  20,
		filterOffset: 80,
		filterLength: 10,
	})

	assertCorruptFooter(t, path)
}

func createRawFooterFile(
	t *testing.T,
	dataSize int,
	meta footerMetadata,
) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "table.sst")

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := file.Write(make([]byte, dataSize)); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := writeFooter(
		file,
		meta.indexOffset,
		meta.indexLength,
		meta.filterOffset,
		meta.filterLength,
	); err != nil {
		file.Close()
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	return path
}

func createValidFooterFile(
	t *testing.T,
	indexOffset uint64,
	indexLength uint64,
	filterOffset uint64,
	filterLength uint64,
) string {
	t.Helper()

	var dataSize uint64
	if filterLength > 0 {
		dataSize = filterOffset + filterLength
	} else {
		dataSize = indexOffset + indexLength
	}

	return createRawFooterFile(t, int(dataSize), footerMetadata{
		indexOffset:  indexOffset,
		indexLength:  indexLength,
		filterOffset: filterOffset,
		filterLength: filterLength,
	})
}

func assertCorruptFooter(t *testing.T, path string) {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	_, err = readFooter(file)
	if !errors.Is(err, ErrCorruptSSTable) {
		t.Fatalf(
			"readFooter error = %v, want %v",
			err,
			ErrCorruptSSTable,
		)
	}
}
