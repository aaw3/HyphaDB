package wal

import (
	"encoding/gob"
	"io"
	"os"

	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
)

type WAL struct {
	file    *os.File
	encoder *gob.Encoder
}

func New(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return nil, err
	}

	return &WAL{
		file:    file,
		encoder: gob.NewEncoder(file),
	}, nil
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

func Replay(path string) (*memtable.MemTable, error) {
	mt := memtable.New()
	file, err := os.Open(path)

	if err != nil {
		if os.IsNotExist(err) {
			return mt, nil
		}
		return nil, err
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
			return nil, err
		}
		mt.Put(record)
	}
	return mt, nil
}
