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
	ID      int
	file    *os.File
	Path    string
	encoder *gob.Encoder
}

type Segment struct {
	ID   int
	Path string
}

func SegmentPath(id int) string {
	return fmt.Sprintf("wal-%d.log", id)
}

func NewSegment(id int) (*WAL, error) {
	path := SegmentPath(id)
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

func RemoveSegment(id int) error {
	err := os.Remove(SegmentPath(id))
	// file already deleted
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func ListSegments() ([]Segment, error) {
	// use glob to find all matching wal segments
	matches, err := filepath.Glob("wal-*.log")
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

func parseSegmentID(path string) (int, bool) {
	base := filepath.Base(path)

	if !strings.HasPrefix(base, "wal-") || !strings.HasSuffix(base, ".log") {
		return 0, false
	}

	idPart := strings.TrimSuffix(strings.TrimPrefix(base, "wal-"), ".log")

	id, err := strconv.Atoi(idPart)
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
	for {
		var record record.Record
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				// EOF
				break
			}
			return err
		}
		mt.Put(record)
	}
	return nil
}

func (w *WAL) Close() error {
	if w.file == nil {
		return nil
	}

	return w.file.Close()
}
