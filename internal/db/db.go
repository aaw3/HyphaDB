package db

import (
	"fmt"

	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/sstable"
)

type DB[K comparable, V any] struct {
	memtable        *memtable.MemTable[K, V]
	maxMemtableSize int
	memTableSize    int
	sstables        []*sstable.SSTable[K, V]
	sstableCounter  int
}

func New[K comparable, V any](maxMemtableSize int) (*DB[K, V], error) {
	sst := make([]*sstable.SSTable[K, V], 0)
	mt := memtable.New[K, V]()

	return &DB[K, V]{
		memtable:        mt,
		maxMemtableSize: maxMemtableSize,
		sstables:        sst,
	}, nil
}

func (db *DB[K, V]) Get(key K) (V, error) {
	if val, exists := db.memtable.Get(key); exists {
		return val, nil
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		sst := db.sstables[i]
		val, err := sst.Open(key)

		if err != nil {
			var zero V

			if err == sstable.ErrDeleted {
				return zero, sstable.ErrNotFound
			}

			if err == sstable.ErrNotFound {
				continue
			}

			return zero, err
		}

		return val, nil
	}

	var zero V
	return zero, sstable.ErrNotFound
}

func (db *DB[K, V]) Put(key K, value V) error {
	db.memtable.Put(key, value)
	db.memTableSize++

	if db.memTableSize >= db.maxMemtableSize {
		if err := db.flushMemtable(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB[K, V]) flushMemtable() error {
	sstablePath := fmt.Sprintf("data-%d.sst", db.sstableCounter)
	sst, err := sstable.CreateFromMemTable(db.memtable, sstablePath)
	if err != nil {
		return err
	}

	db.sstables = append(db.sstables, sst)
	db.sstableCounter++
	db.memtable = memtable.New[K, V]()
	db.memTableSize = 0

	return nil
}
