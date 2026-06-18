package db

import (
	"errors"

	"github.com/aaw3/hyphadb/internal/memtable"
)

type DB[K comparable, V any] struct {
	memtable *memtable.MemTable[K, V]
}

func New[K comparable, V any]() (*DB[K, V], error) {
	mt := memtable.New[K, V]()

	return &DB[K, V]{
		memtable: mt,
	}, nil
}

func (db *DB[K, V]) Get(key K) (V, error) {
	if val, exists := db.memtable.Get(key); exists {
		return val, nil
	}

	var zero V
	return zero, errors.New("key not found")
}

func (db *DB[K, V]) Put(key K, value V) error {
	db.memtable.Put(key, value)
	return nil
}
