package manifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/aaw3/hyphadb/internal/fsutil"
)

type Manifest struct {
	NextSSTableID    uint64
	NextWALSegmentID uint64
	SSTables         []SSTableMetadata
}

type SSTableMetadata struct {
	ID          uint64
	Path        string
	Level       uint32
	SizeBytes   uint64
	SmallestKey string
	LargestKey  string
}

const (
	manifestVersion uint32 = 1

	manifestHeaderSize        = 8 + 4 + 4 + 4
	manifestPayloadHeaderSize = 8 + 8 + 4
	sstableFixedSize          = 8 + 4 + 8 + 4 + 4 + 4

	maxManifestPayloadSize = 256 * 1024 * 1024
	maxSSTableCount        = 1_000_000
	maxMetadataStringSize  = 64 * 1024
)

var (
	manifestMagic       = [8]byte{'H', 'Y', 'P', 'H', 'A', 'M', 'A', 'N'}
	manifestCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

	ErrCorruptManifest            = errors.New("corrupt manifest")
	ErrUnsupportedManifestVersion = errors.New("unsupported manifest version")

	syncParent = fsutil.SyncParent
)

// PublishError reports a failure after the new manifest was renamed into place.
// Callers must retain the state described by the manifest because it is
// visible in the current filesystem namespace, even though its directory
// entry could not be confirmed durable.
type PublishError struct {
	Err error
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("manifest published but directory sync failed: %v", e.Err)
}

func (e *PublishError) Unwrap() error {
	return e.Err
}

func IsPublished(err error) bool {
	var publishErr *PublishError
	return errors.As(err, &publishErr)
}

func Read(path string) (*Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return emptyManifest(), nil
		}
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(
		file,
		int64(manifestHeaderSize+maxManifestPayloadSize+1),
	))
	if err != nil {
		return nil, err
	}
	if len(data) > manifestHeaderSize+maxManifestPayloadSize {
		return nil, fmt.Errorf(
			"%w: file exceeds maximum size",
			ErrCorruptManifest,
		)
	}

	m, err := decode(data)
	if err != nil {
		return nil, err
	}
	if err := ensureSafeNextSSTableID(m); err != nil {
		return nil, err
	}
	return m, nil
}

func emptyManifest() *Manifest {
	return &Manifest{SSTables: []SSTableMetadata{}}
}

func ensureSafeNextSSTableID(m *Manifest) error {
	var maxID uint64
	hasTables := false

	for _, table := range m.SSTables {
		if !hasTables || table.ID > maxID {
			maxID = table.ID
			hasTables = true
		}
	}

	if hasTables && m.NextSSTableID <= maxID {
		if maxID == math.MaxUint64 {
			return fmt.Errorf(
				"%w: SSTable identifier space exhausted",
				ErrCorruptManifest,
			)
		}
		m.NextSSTableID = maxID + 1
	}
	return nil
}

func Write(path string, manifest *Manifest) error {
	data, err := encode(manifest)
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := syncParent(path); err != nil {
		return &PublishError{Err: err}
	}
	return nil
}

func encode(m *Manifest) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("manifest is nil")
	}
	if len(m.SSTables) > maxSSTableCount {
		return nil, fmt.Errorf(
			"SSTable count %d exceeds maximum %d",
			len(m.SSTables),
			maxSSTableCount,
		)
	}

	payloadSize := manifestPayloadHeaderSize
	for i, table := range m.SSTables {
		stringsSize, err := metadataStringsSize(table)
		if err != nil {
			return nil, fmt.Errorf("SSTable %d: %w", i, err)
		}
		payloadSize += sstableFixedSize + stringsSize
		if payloadSize > maxManifestPayloadSize {
			return nil, fmt.Errorf(
				"manifest payload exceeds maximum size %d",
				maxManifestPayloadSize,
			)
		}
	}

	payload := bytes.NewBuffer(make([]byte, 0, payloadSize))
	writeUint64(payload, m.NextSSTableID)
	writeUint64(payload, m.NextWALSegmentID)
	writeUint32(payload, uint32(len(m.SSTables)))
	for _, table := range m.SSTables {
		writeUint64(payload, table.ID)
		writeUint32(payload, table.Level)
		writeUint64(payload, table.SizeBytes)
		writeString(payload, table.Path)
		writeString(payload, table.SmallestKey)
		writeString(payload, table.LargestKey)
	}

	var header [manifestHeaderSize]byte
	copy(header[:8], manifestMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], manifestVersion)
	binary.LittleEndian.PutUint32(header[12:16], uint32(payload.Len()))
	binary.LittleEndian.PutUint32(
		header[16:20],
		manifestChecksum(header[:16], payload.Bytes()),
	)

	encoded := make([]byte, 0, len(header)+payload.Len())
	encoded = append(encoded, header[:]...)
	encoded = append(encoded, payload.Bytes()...)
	return encoded, nil
}

func decode(data []byte) (*Manifest, error) {
	if len(data) < manifestHeaderSize {
		return nil, fmt.Errorf("%w: file is too small", ErrCorruptManifest)
	}
	if !bytes.Equal(data[:8], manifestMagic[:]) {
		return nil, fmt.Errorf("%w: invalid file magic", ErrCorruptManifest)
	}

	version := binary.LittleEndian.Uint32(data[8:12])
	if version != manifestVersion {
		return nil, fmt.Errorf(
			"%w: %d",
			ErrUnsupportedManifestVersion,
			version,
		)
	}
	payloadLen := binary.LittleEndian.Uint32(data[12:16])
	if payloadLen > maxManifestPayloadSize {
		return nil, fmt.Errorf(
			"%w: payload length %d exceeds maximum %d",
			ErrCorruptManifest,
			payloadLen,
			maxManifestPayloadSize,
		)
	}
	wantSize := uint64(manifestHeaderSize) + uint64(payloadLen)
	if uint64(len(data)) != wantSize {
		return nil, fmt.Errorf(
			"%w: file size %d does not match encoded size %d",
			ErrCorruptManifest,
			len(data),
			wantSize,
		)
	}

	payload := data[manifestHeaderSize:]
	wantChecksum := binary.LittleEndian.Uint32(data[16:20])
	gotChecksum := manifestChecksum(data[:16], payload)
	if wantChecksum != gotChecksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorruptManifest)
	}

	r := bytes.NewReader(payload)
	if r.Len() < manifestPayloadHeaderSize {
		return nil, fmt.Errorf("%w: payload is too small", ErrCorruptManifest)
	}
	nextSSTableID, err := readUint64(r)
	if err != nil {
		return nil, corruptReadError("next SSTable ID", err)
	}
	nextWALSegmentID, err := readUint64(r)
	if err != nil {
		return nil, corruptReadError("next WAL segment ID", err)
	}
	tableCount, err := readUint32(r)
	if err != nil {
		return nil, corruptReadError("SSTable count", err)
	}
	if tableCount > maxSSTableCount {
		return nil, fmt.Errorf(
			"%w: SSTable count %d exceeds maximum %d",
			ErrCorruptManifest,
			tableCount,
			maxSSTableCount,
		)
	}
	if uint64(tableCount)*sstableFixedSize > uint64(r.Len()) {
		return nil, fmt.Errorf(
			"%w: %d SSTable entries cannot fit in %d bytes",
			ErrCorruptManifest,
			tableCount,
			r.Len(),
		)
	}

	m := &Manifest{
		NextSSTableID:    nextSSTableID,
		NextWALSegmentID: nextWALSegmentID,
		SSTables:         make([]SSTableMetadata, 0, tableCount),
	}
	for i := uint32(0); i < tableCount; i++ {
		table, err := decodeSSTableMetadata(r)
		if err != nil {
			return nil, fmt.Errorf("%w: SSTable %d: %v", ErrCorruptManifest, i, err)
		}
		m.SSTables = append(m.SSTables, table)
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf(
			"%w: payload has %d trailing bytes",
			ErrCorruptManifest,
			r.Len(),
		)
	}
	return m, nil
}

func decodeSSTableMetadata(r *bytes.Reader) (SSTableMetadata, error) {
	id, err := readUint64(r)
	if err != nil {
		return SSTableMetadata{}, err
	}
	level, err := readUint32(r)
	if err != nil {
		return SSTableMetadata{}, err
	}
	sizeBytes, err := readUint64(r)
	if err != nil {
		return SSTableMetadata{}, err
	}
	path, err := readString(r)
	if err != nil {
		return SSTableMetadata{}, fmt.Errorf("path: %w", err)
	}
	smallestKey, err := readString(r)
	if err != nil {
		return SSTableMetadata{}, fmt.Errorf("smallest key: %w", err)
	}
	largestKey, err := readString(r)
	if err != nil {
		return SSTableMetadata{}, fmt.Errorf("largest key: %w", err)
	}
	return SSTableMetadata{
		ID:          id,
		Path:        path,
		Level:       level,
		SizeBytes:   sizeBytes,
		SmallestKey: smallestKey,
		LargestKey:  largestKey,
	}, nil
}

func metadataStringsSize(table SSTableMetadata) (int, error) {
	fields := [...]struct {
		name  string
		value string
	}{
		{name: "path", value: table.Path},
		{name: "smallest key", value: table.SmallestKey},
		{name: "largest key", value: table.LargestKey},
	}

	total := 0
	for _, field := range fields {
		name, value := field.name, field.value
		if len(value) > maxMetadataStringSize {
			return 0, fmt.Errorf(
				"%s length %d exceeds maximum %d",
				name,
				len(value),
				maxMetadataStringSize,
			)
		}
		total += len(value)
	}
	return total, nil
}

func writeString(w *bytes.Buffer, value string) {
	writeUint32(w, uint32(len(value)))
	_, _ = w.WriteString(value)
}

func readString(r *bytes.Reader) (string, error) {
	length, err := readUint32(r)
	if err != nil {
		return "", err
	}
	if length > maxMetadataStringSize {
		return "", fmt.Errorf(
			"length %d exceeds maximum %d",
			length,
			maxMetadataStringSize,
		)
	}
	if uint64(length) > uint64(r.Len()) {
		return "", fmt.Errorf(
			"length %d exceeds remaining payload %d",
			length,
			r.Len(),
		)
	}
	value := make([]byte, int(length))
	if _, err := io.ReadFull(r, value); err != nil {
		return "", err
	}
	return string(value), nil
}

func writeUint32(w *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	_, _ = w.Write(encoded[:])
}

func writeUint64(w *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	_, _ = w.Write(encoded[:])
}

func readUint32(r io.Reader) (uint32, error) {
	var encoded [4]byte
	if _, err := io.ReadFull(r, encoded[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(encoded[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var encoded [8]byte
	if _, err := io.ReadFull(r, encoded[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(encoded[:]), nil
}

func manifestChecksum(prefix, payload []byte) uint32 {
	checksum := crc32.New(manifestCRC32CTable)
	_, _ = checksum.Write(prefix)
	_, _ = checksum.Write(payload)
	return checksum.Sum32()
}

func corruptReadError(field string, err error) error {
	return fmt.Errorf("%w: read %s: %v", ErrCorruptManifest, field, err)
}
