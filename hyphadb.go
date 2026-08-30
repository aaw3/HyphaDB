// Package hyphadb provides the public embedded database API.
package hyphadb

import (
	internaldb "github.com/aaw3/hyphadb/internal/db"
	"github.com/aaw3/hyphadb/internal/sstable"
)

var (
	ErrClosed         = internaldb.ErrClosed
	ErrNotFound       = sstable.ErrNotFound
	ErrSnapshotClosed = internaldb.ErrSnapshotClosed
	ErrBatchClosed    = internaldb.ErrBatchClosed
)

type Options struct {
	DataDir    string
	Memtable   MemtableOptions
	Compaction CompactionOptions
	BlockCache BlockCacheOptions
}

type MemtableOptions struct {
	MaxEntries int
}

type CompactionOptions struct {
	TableCountThreshold int
}

type BlockCacheOptions struct {
	CapacityBytes int
}

type DB struct {
	inner *internaldb.DB
}

func New(maxMemtableSize, compactionThreshold int) (*DB, error) {
	return Open(Options{
		Memtable: MemtableOptions{
			MaxEntries: maxMemtableSize,
		},
		Compaction: CompactionOptions{
			TableCountThreshold: compactionThreshold,
		},
	})
}

func Open(opts Options) (*DB, error) {
	inner, err := internaldb.Open(internaldb.Options{
		DataDir: opts.DataDir,
		Memtable: internaldb.MemtableOptions{
			MaxEntries: opts.Memtable.MaxEntries,
		},
		Compaction: internaldb.CompactionOptions{
			TableCountThreshold: opts.Compaction.TableCountThreshold,
		},
		BlockCache: internaldb.BlockCacheOptions{
			CapacityBytes: opts.BlockCache.CapacityBytes,
		},
	})
	if err != nil {
		return nil, err
	}
	return &DB{inner: inner}, nil
}

func (db *DB) Get(key string) ([]byte, error) {
	value, err := db.inner.Get(key)
	if err != nil {
		return nil, err
	}
	return cloneBytes(value), nil
}

func (db *DB) Put(key string, value []byte) error {
	return db.inner.Put(key, cloneBytes(value))
}

func (db *DB) Delete(key string) error {
	return db.inner.Delete(key)
}

func (db *DB) NewBatch() *Batch {
	return &Batch{inner: db.inner.NewBatch()}
}

func (db *DB) NewSnapshot() (*Snapshot, error) {
	snapshot, err := db.inner.NewSnapshot()
	if err != nil {
		return nil, err
	}
	return &Snapshot{inner: snapshot}, nil
}

func (db *DB) NewIterator(opts IteratorOptions) (*Iterator, error) {
	iterator, err := db.inner.NewIterator(internaldb.IteratorOptions{
		Start: opts.Start,
		End:   opts.End,
	})
	if err != nil {
		return nil, err
	}
	return &Iterator{inner: iterator}, nil
}

func (db *DB) ScanPrefix(prefix string) (*Iterator, error) {
	iterator, err := db.inner.ScanPrefix(prefix)
	if err != nil {
		return nil, err
	}
	return &Iterator{inner: iterator}, nil
}

func (db *DB) Compact() error {
	return db.inner.Compact()
}

func (db *DB) Close() error {
	return db.inner.Close()
}

type WriteOptions struct {
	Sync bool
}

type Batch struct {
	inner *internaldb.Batch
}

func (batch *Batch) Put(key string, value []byte) error {
	return batch.inner.Put(key, cloneBytes(value))
}

func (batch *Batch) Delete(key string) error {
	return batch.inner.Delete(key)
}

func (batch *Batch) Commit(opts WriteOptions) error {
	return batch.inner.Commit(internaldb.WriteOptions{Sync: opts.Sync})
}

func (batch *Batch) Cancel() error {
	return batch.inner.Cancel()
}

type Snapshot struct {
	inner *internaldb.Snapshot
}

func (snapshot *Snapshot) Get(key string) ([]byte, error) {
	value, err := snapshot.inner.Get(key)
	if err != nil {
		return nil, err
	}
	return cloneBytes(value), nil
}

func (snapshot *Snapshot) NewIterator(opts IteratorOptions) (*Iterator, error) {
	iterator, err := snapshot.inner.NewIterator(internaldb.IteratorOptions{
		Start: opts.Start,
		End:   opts.End,
	})
	if err != nil {
		return nil, err
	}
	return &Iterator{inner: iterator}, nil
}

func (snapshot *Snapshot) ScanPrefix(prefix string) (*Iterator, error) {
	iterator, err := snapshot.inner.ScanPrefix(prefix)
	if err != nil {
		return nil, err
	}
	return &Iterator{inner: iterator}, nil
}

func (snapshot *Snapshot) Sequence() uint64 {
	return snapshot.inner.Sequence()
}

func (snapshot *Snapshot) Close() error {
	return snapshot.inner.Close()
}

type IteratorOptions struct {
	Start string
	End   string
}

type Iterator struct {
	inner *internaldb.Iterator
}

func (iterator *Iterator) Next() bool {
	return iterator.inner.Next()
}

func (iterator *Iterator) Key() string {
	return iterator.inner.Record().Key
}

func (iterator *Iterator) Value() []byte {
	return cloneBytes(iterator.inner.Record().Value)
}

func (iterator *Iterator) Err() error {
	return iterator.inner.Err()
}

func (iterator *Iterator) Close() error {
	return iterator.inner.Close()
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}
