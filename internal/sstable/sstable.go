package sstable

import (
	"encoding/gob"
	"errors"
	"io"
	"os"
	"sort"

	"github.com/aaw3/hyphadb/internal/memtable"
)

const TOMBSTONE = "__DELETED__"

var ErrNotFound = errors.New("key not found")
var ErrDeleted = errors.New("key has been deleted")

type Pair[K comparable, V any] struct {
	Key   K
	Value V
}

type SSTable[K comparable, V any] struct {
	Path  string
	Pairs []Pair[K, V]
}

func CreateFromMemTable[K comparable, V any](mt *memtable.MemTable[K, V], path string) (*SSTable[K, V], error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	entries := mt.Entries()

	pairs := make([]Pair[K, V], 0, len(entries))
	for k, v := range entries {
		pairs = append(pairs, Pair[K, V]{Key: k, Value: v})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return any(pairs[i].Key).(string) < any(pairs[j].Key).(string)
	})

	encoder := gob.NewEncoder(file)
	for _, pair := range pairs {
		if err := encoder.Encode(pair); err != nil {
			return nil, err
		}
	}

	return &SSTable[K, V]{
		Path:  path,
		Pairs: pairs,
	}, nil
}

func (s *SSTable[K, V]) Open(key K) (V, error) {
	file, err := os.Open(s.Path)
	if err != nil {
		var zero V
		return zero, err
	}

	decoder := gob.NewDecoder(file)
	for {
		var pair Pair[K, V]
		if err := decoder.Decode(&pair); err != nil {
			if err == io.EOF {
				break
			}
			var zero V
			return zero, err
		}

		// Assume key is string, this breaks generics
		keyInDB := any(pair.Key).(string)
		if keyInDB == any(key).(string) {
			if any(pair.Value).(string) == TOMBSTONE {
				var zero V
				return zero, ErrDeleted
			}
			return pair.Value, nil
		}

		if keyInDB > any(key).(string) {
			var zero V
			return zero, ErrNotFound
		}
	}

	var zero V
	return zero, ErrNotFound
}
