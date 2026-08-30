package db

import (
	"errors"

	"github.com/aaw3/hyphadb/internal/record"
)

var ErrBatchClosed = errors.New("batch is closed")

type BatchOperation struct {
	Key     string
	Value   []byte
	Deleted bool
}

type WriteOptions struct {
	Sync bool
}

type Batch struct {
	db         *DB
	operations []BatchOperation
	closed     bool
}

func (db *DB) NewBatch() *Batch {
	return &Batch{db: db}
}

func (b *Batch) Put(key string, value []byte) error {
	if b.closed {
		return ErrBatchClosed
	}
	b.operations = append(b.operations, BatchOperation{
		Key:   key,
		Value: append([]byte(nil), value...),
	})
	return nil
}

func (b *Batch) Delete(key string) error {
	if b.closed {
		return ErrBatchClosed
	}
	b.operations = append(b.operations, BatchOperation{Key: key, Deleted: true})
	return nil
}

func (b *Batch) Commit(opts WriteOptions) error {
	if b.closed {
		return ErrBatchClosed
	}
	if err := b.db.applyBatch(b.operations, opts); err != nil {
		return err
	}
	b.closed = true
	return nil
}

func (b *Batch) Cancel() error {
	if b.closed {
		return nil
	}
	b.closed = true
	b.operations = nil
	return nil
}

func (db *DB) applyBatch(operations []BatchOperation, opts WriteOptions) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	batchID := db.nextSeq
	records := make([]record.Record, len(operations))
	for i, operation := range operations {
		records[i] = record.Record{
			Key: operation.Key,
			Seq: db.nextSeq + uint64(i),
			Entry: record.Entry{
				Value:   append([]byte(nil), operation.Value...),
				Deleted: operation.Deleted,
			},
		}
	}

	if err := db.wal.WriteBatch(batchID, records, opts.Sync); err != nil {
		return err
	}

	for _, rec := range records {
		db.memtable.Put(rec)
		db.memTableSize++
	}
	db.nextSeq += uint64(len(records))
	if len(records) == 0 {
		db.nextSeq++
	}

	if db.memTableSize >= db.maxMemtableSize {
		return db.rotateMemtable()
	}
	return nil
}
