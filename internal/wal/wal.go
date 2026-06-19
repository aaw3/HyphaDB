package wal

import (
	"encoding/gob"
	"io"
	"os"

	"github.com/aaw3/hyphadb/internal/memtable"
)

type WALEntry[K comparable, V any] struct {
	Key   K
	Value V
}

type WAL[K comparable, V any] struct {
	file    *os.File
	encoder *gob.Encoder
}

func New[K comparable, V any](path string) (*WAL[K, V], error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return nil, err
	}

	return &WAL[K, V]{
		file:    file,
		encoder: gob.NewEncoder(file),
	}, nil
}

func (wal *WAL[K, V]) Write(key K, value V) error {
	entry := WALEntry[K, V]{
		Key:   key,
		Value: value,
	}
	return wal.encoder.Encode(&entry)
}

func Replay[K comparable, V any](path string) (*memtable.MemTable[K, V], error) {
	mt := memtable.New[K, V]()
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
		var entry WALEntry[K, V]
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				// EOF
				break
			}
			return nil, err
		}
		mt.Put(entry.Key, entry.Value)
	}
	return mt, nil
}
