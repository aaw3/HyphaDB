package sstable

import (
	"encoding/gob"
	"errors"
	"io"
	"os"

	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
)

var ErrNotFound = errors.New("key not found")
var ErrDeleted = errors.New("key has been deleted")

type SSTable struct {
	Path    string
	Records []record.Record
}

func CreateFromMemTable(mt *memtable.MemTable, path string) (*SSTable, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	records := mt.Records()

	encoder := gob.NewEncoder(file)
	for _, rec := range records {
		if err := encoder.Encode(rec); err != nil {
			return nil, err
		}
	}

	return &SSTable{
		Path:    path,
		Records: records,
	}, nil
}

func (s *SSTable) Open(key string) ([]byte, error) {
	file, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	for {
		var record record.Record
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if record.Key == key {
			if record.Entry.Deleted {
				return nil, ErrDeleted
			}
			return record.Entry.Value, nil
		}

		if record.Key > key {
			return nil, ErrNotFound
		}
	}

	return nil, ErrNotFound
}

func (s *SSTable) MaxSeq() (uint64, error) {
	file, err := os.Open(s.Path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var maxSeq uint64
	decoder := gob.NewDecoder(file)
	for {
		var rec record.Record
		if err := decoder.Decode(&rec); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}

		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
	}

	return maxSeq, nil
}
