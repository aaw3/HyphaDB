package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/aaw3/hyphadb/internal/fsutil"
	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
)

type WAL struct {
	ID   uint64
	file *os.File
	Path string
}

type Segment struct {
	ID   uint64
	Path string
}

// ReplayStats describes identifiers consumed by records in a WAL segment,
// including records from incomplete batches that are not applied.
type ReplayStats struct {
	MaxSequence uint64
}

const (
	walVersion        uint32 = 1
	walHeaderSize            = 8 + 4
	frameHeaderSize          = 4 + 4
	batchMetadataSize        = 1 + 8
	maxFramePayload          = batchMetadataSize + record.HeaderSize +
		record.MaxKeySize + record.MaxValueSize
)

var (
	walMagic       = [8]byte{'H', 'Y', 'P', 'H', 'A', 'W', 'A', 'L'}
	walCRC32CTable = crc32.MakeTable(crc32.Castagnoli)
	ErrCorruptWAL  = errors.New("corrupt WAL")
)

func SegmentPath(id uint64) string {
	return SegmentPathInDir(".", id)
}

func NewSegment(id uint64) (*WAL, error) {
	return NewSegmentInDir(".", id)
}

func SegmentPathInDir(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%d.log", id))
}

func NewSegmentInDir(dir string, id uint64) (*WAL, error) {
	path := SegmentPathInDir(dir, id)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Size() == 0 {
		if err := writeWALHeader(file); err != nil {
			file.Close()
			return nil, err
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, err
		}
		if err := fsutil.SyncParent(path); err != nil {
			file.Close()
			return nil, err
		}
	} else if err := validateWALHeader(file); err != nil {
		file.Close()
		return nil, err
	}

	return &WAL{
		ID:   id,
		file: file,
		Path: path,
	}, nil
}

func writeWALHeader(w io.Writer) error {
	var header [walHeaderSize]byte
	copy(header[:8], walMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], walVersion)
	_, err := w.Write(header[:])
	return err
}

func validateWALHeader(r io.ReaderAt) error {
	var header [walHeaderSize]byte
	if _, err := r.ReadAt(header[:], 0); err != nil {
		return fmt.Errorf("%w: read header: %v", ErrCorruptWAL, err)
	}
	if !bytes.Equal(header[:8], walMagic[:]) {
		return fmt.Errorf("%w: invalid file magic", ErrCorruptWAL)
	}
	version := binary.LittleEndian.Uint32(header[8:12])
	if version != walVersion {
		return fmt.Errorf(
			"%w: unsupported version %d",
			ErrCorruptWAL,
			version,
		)
	}
	return nil
}

func RemoveSegment(id uint64) error {
	return RemoveSegmentInDir(".", id)
}

func RemoveSegmentInDir(dir string, id uint64) error {
	path := SegmentPathInDir(dir, id)
	err := os.Remove(path)
	// file already deleted
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return fsutil.SyncParent(path)
}

func ListSegments() ([]Segment, error) {
	return ListSegmentsInDir(".")
}

func ListSegmentsInDir(dir string) ([]Segment, error) {
	// use glob to find all matching wal segments
	matches, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil {
		return nil, err
	}

	segments := make([]Segment, 0, len(matches))
	for _, path := range matches {
		id, ok := parseSegmentID(path)
		if !ok {
			continue
		}

		segments = append(segments, Segment{
			ID:   id,
			Path: path,
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID < segments[j].ID
	})

	return segments, nil
}

func parseSegmentID(path string) (uint64, bool) {
	base := filepath.Base(path)

	if !strings.HasPrefix(base, "wal-") || !strings.HasSuffix(base, ".log") {
		return 0, false
	}

	idPart := strings.TrimSuffix(strings.TrimPrefix(base, "wal-"), ".log")

	id, err := strconv.ParseUint(idPart, 10, 64)
	if err != nil {
		return 0, false
	}

	return id, true
}

func (w *WAL) Write(key string, seq uint64, value []byte) error {
	return w.WriteRecord(record.Record{
		Key: key,
		Seq: seq,
		Entry: record.Entry{
			Value:   value,
			Deleted: false,
		},
	})
}

func (w *WAL) WriteRecord(rec record.Record) error {
	payload, err := encodeRecord(rec)
	if err != nil {
		return err
	}

	var header [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(
		header[4:8],
		frameChecksum(header[0:4], payload),
	)

	frame := make([]byte, 0, len(header)+len(payload))
	frame = append(frame, header[:]...)
	frame = append(frame, payload...)
	if _, err := w.file.Write(frame); err != nil {
		return err
	}
	return nil
}

func encodeRecord(rec record.Record) ([]byte, error) {
	if len(rec.Key) > record.MaxKeySize {
		return nil, fmt.Errorf("key length %d exceeds maximum", len(rec.Key))
	}
	if len(rec.Value) > record.MaxValueSize {
		return nil, fmt.Errorf("value length %d exceeds maximum", len(rec.Value))
	}
	if rec.BatchKind > record.BatchCommit {
		return nil, fmt.Errorf("invalid batch kind %d", rec.BatchKind)
	}

	payload := bytes.NewBuffer(make([]byte, 0, batchMetadataSize+record.EncodedSize(rec)))
	payload.WriteByte(byte(rec.BatchKind))
	var batchID [8]byte
	binary.LittleEndian.PutUint64(batchID[:], rec.BatchID)
	payload.Write(batchID[:])
	if err := record.EncodeBinary(payload, rec); err != nil {
		return nil, err
	}
	return payload.Bytes(), nil
}

func decodeRecord(payload []byte) (record.Record, error) {
	if len(payload) < batchMetadataSize+record.HeaderSize {
		return record.Record{}, fmt.Errorf("record payload is too small")
	}

	kind := record.BatchKind(payload[0])
	if kind > record.BatchCommit {
		return record.Record{}, fmt.Errorf("invalid batch kind %d", kind)
	}
	batchID := binary.LittleEndian.Uint64(payload[1:9])
	r := bytes.NewReader(payload[batchMetadataSize:])
	rec, err := record.DecodeBinary(r)
	if err != nil {
		return record.Record{}, err
	}
	if r.Len() != 0 {
		return record.Record{}, fmt.Errorf("record payload has %d trailing bytes", r.Len())
	}
	rec.BatchID = batchID
	rec.BatchKind = kind
	return rec, nil
}

func frameChecksum(length, payload []byte) uint32 {
	checksum := crc32.New(walCRC32CTable)
	_, _ = checksum.Write(length)
	_, _ = checksum.Write(payload)
	return checksum.Sum32()
}

func (w *WAL) WriteBatch(batchID uint64, records []record.Record, sync bool) error {
	if err := w.WriteRecord(record.Record{
		BatchID:   batchID,
		BatchKind: record.BatchBegin,
	}); err != nil {
		return err
	}
	for _, rec := range records {
		rec.BatchID = batchID
		rec.BatchKind = record.BatchOperation
		if err := w.WriteRecord(rec); err != nil {
			return err
		}
	}
	if err := w.WriteRecord(record.Record{
		BatchID:   batchID,
		BatchKind: record.BatchCommit,
	}); err != nil {
		return err
	}
	if sync {
		return w.file.Sync()
	}
	return nil
}

func ReplayInto(path string, mt *memtable.MemTable) error {
	_, err := ReplayIntoWithStats(path, mt)
	return err
}

func ReplayIntoWithStats(path string, mt *memtable.MemTable) (ReplayStats, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)

	if err != nil {
		if os.IsNotExist(err) {
			return ReplayStats{}, nil
		}
		return ReplayStats{}, err
	}
	defer file.Close()

	var stats ReplayStats
	info, err := file.Stat()
	if err != nil {
		return ReplayStats{}, err
	}
	if info.Size() == 0 {
		return stats, nil
	}
	if err := validateWALHeader(file); err != nil {
		return ReplayStats{}, err
	}
	if _, err := file.Seek(walHeaderSize, io.SeekStart); err != nil {
		return ReplayStats{}, err
	}

	validOffset := int64(walHeaderSize)
	pending := make(map[uint64][]record.Record)
	for {
		var header [frameHeaderSize]byte
		_, err := io.ReadFull(file, header[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			if err := file.Truncate(validOffset); err != nil {
				return ReplayStats{}, err
			}
			break
		}
		if err != nil {
			return ReplayStats{}, err
		}

		payloadLen := binary.LittleEndian.Uint32(header[0:4])
		if payloadLen > maxFramePayload {
			return ReplayStats{}, fmt.Errorf(
				"%w: frame length %d exceeds maximum %d",
				ErrCorruptWAL,
				payloadLen,
				maxFramePayload,
			)
		}
		payload := make([]byte, int(payloadLen))
		if _, err := io.ReadFull(file, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if err := file.Truncate(validOffset); err != nil {
					return ReplayStats{}, err
				}
				break
			}
			return ReplayStats{}, err
		}

		wantChecksum := binary.LittleEndian.Uint32(header[4:8])
		gotChecksum := frameChecksum(header[0:4], payload)
		if wantChecksum != gotChecksum {
			return ReplayStats{}, fmt.Errorf(
				"%w: frame checksum mismatch at offset %d",
				ErrCorruptWAL,
				validOffset,
			)
		}
		rec, err := decodeRecord(payload)
		if err != nil {
			return ReplayStats{}, fmt.Errorf(
				"%w: decode frame at offset %d: %v",
				ErrCorruptWAL,
				validOffset,
				err,
			)
		}
		validOffset += int64(frameHeaderSize) + int64(payloadLen)
		if rec.Seq > stats.MaxSequence {
			stats.MaxSequence = rec.Seq
		}
		if rec.BatchID > stats.MaxSequence {
			stats.MaxSequence = rec.BatchID
		}
		switch rec.BatchKind {
		case record.BatchNone:
			mt.Put(rec)
		case record.BatchBegin:
			pending[rec.BatchID] = nil
		case record.BatchOperation:
			if _, ok := pending[rec.BatchID]; ok {
				pending[rec.BatchID] = append(pending[rec.BatchID], rec)
			}
		case record.BatchCommit:
			if records, ok := pending[rec.BatchID]; ok {
				for _, operation := range records {
					operation.BatchID = 0
					operation.BatchKind = record.BatchNone
					mt.Put(operation)
				}
				delete(pending, rec.BatchID)
			}
		}
	}
	return stats, nil
}

func (w *WAL) Close() error {
	if w.file == nil {
		return nil
	}

	return w.file.Close()
}
