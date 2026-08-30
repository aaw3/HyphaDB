package wal

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
)

type WAL struct {
	ID      uint64
	file    *os.File
	Path    string
	encoder *gob.Encoder
}

type Segment struct {
	ID   uint64
	Path string
}

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
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return nil, err
	}

	return &WAL{
		ID:      id,
		file:    file,
		Path:    path,
		encoder: gob.NewEncoder(file),
	}, nil
}

func RemoveSegment(id uint64) error {
	return RemoveSegmentInDir(".", id)
}

func RemoveSegmentInDir(dir string, id uint64) error {
	err := os.Remove(SegmentPathInDir(dir, id))
	// file already deleted
	if os.IsNotExist(err) {
		return nil
	}
	return err
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

func (w *WAL) WriteRecord(record record.Record) error {
	return w.encoder.Encode(record)
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
	file, err := os.Open(path)

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	pending := make(map[uint64][]record.Record)
	for {
		var rec record.Record
		if err := decoder.Decode(&rec); err != nil {
			if err == io.EOF {
				// EOF
				break
			}
			return err
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
	return nil
}

func (w *WAL) Close() error {
	if w.file == nil {
		return nil
	}

	return w.file.Close()
}
