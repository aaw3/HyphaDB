package db

import (
	"fmt"
	"log"
	"os"

	"github.com/aaw3/hyphadb/internal/compaction"
	"github.com/aaw3/hyphadb/internal/manifest"
	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
	"github.com/aaw3/hyphadb/internal/wal"
)

type DB struct {
	memtable            *memtable.MemTable
	maxMemtableSize     int
	memTableSize        int
	sstables            []*sstable.SSTable
	wal                 *wal.WAL
	walPath             string
	manifest            *manifest.Manifest
	manifestPath        string
	compactionThreshold int
	nextSeq             uint64
}

func New(maxMemtableSize int, compactionThreshold int) (*DB, error) {
	walPath := "wal.log"
	mt, err := wal.Replay(walPath)
	if err != nil {
		return nil, err
	}

	// open WAL for appending
	w, err := wal.New(walPath)
	if err != nil {
		return nil, err
	}

	manifestPath := "MANIFEST"
	mf, err := manifest.Read(manifestPath)
	if err != nil {
		return nil, err
	}

	sstables := make([]*sstable.SSTable, 0, len(mf.SSTablePaths))
	for _, path := range mf.SSTablePaths {
		sstables = append(sstables, &sstable.SSTable{Path: path})
	}

	return &DB{
		memtable:            mt,
		maxMemtableSize:     maxMemtableSize,
		memTableSize:        len(mt.Records()),
		sstables:            sstables,
		wal:                 w,
		walPath:             walPath,
		manifest:            mf,
		manifestPath:        manifestPath,
		compactionThreshold: compactionThreshold,
	}, nil
}

func (db *DB) Compact() error {
	id := db.manifest.NextSSTableID
	db.manifest.NextSSTableID++
	compactedSSTablePath := fmt.Sprintf("compact-%d.sst", id)
	compactedSSTable, err := compaction.MergeSSTables(db.sstables, compactedSSTablePath)
	if err != nil {
		return err
	}

	// write compacted SSTable to MANIFEST file
	db.manifest.SSTablePaths = []string{compactedSSTablePath}
	if err := manifest.Write(db.manifestPath, db.manifest); err != nil {
		return err
	}

	for _, sst := range db.sstables {
		if err := os.Remove(sst.Path); err != nil {
			log.Printf("Failed while deleting old SSTable %s: %v", sst.Path, err)
		}
	}

	db.sstables = []*sstable.SSTable{compactedSSTable}

	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	if entry, exists := db.memtable.Get(key); exists {

		if entry.Deleted {
			return nil, sstable.ErrNotFound
		}

		return entry.Value, nil
	}

	// Check SSTables in reverse order (newest to oldest)
	for i := len(db.sstables) - 1; i >= 0; i-- {
		val, err := db.sstables[i].Open(key)

		if err != nil {
			if err == sstable.ErrDeleted || err == sstable.ErrNotFound {
				if err == sstable.ErrDeleted {
					return nil, sstable.ErrNotFound
				}
				continue
			}
			return nil, err
		}

		return val, nil
	}
	return nil, sstable.ErrNotFound
}

func (db *DB) Put(key string, value []byte) error {
	seq := db.nextSeq
	db.nextSeq++

	rec := record.Record{
		Key: key,
		Seq: seq,
		Entry: record.Entry{
			Value:   value,
			Deleted: false,
		},
	}

	//write to WAL first
	if err := db.wal.WriteRecord(rec); err != nil {
		return err
	}

	db.memtable.Put(rec)
	db.memTableSize++

	if db.memTableSize >= db.maxMemtableSize {
		return db.flushMemtable()
	}
	return nil
}

func (db *DB) Delete(key string) error {
	seq := db.nextSeq
	db.nextSeq++

	rec := record.Record{
		Key: key,
		Seq: seq,
		Entry: record.Entry{
			Deleted: true,
		},
	}

	// write tombstone to WAL and memtable for quick deletion
	if err := db.wal.WriteRecord(rec); err != nil {
		return err
	}

	db.memtable.Put(rec)
	db.memTableSize++

	if db.memTableSize >= db.maxMemtableSize {
		return db.flushMemtable()
	}

	return nil
}

func (db *DB) flushMemtable() error {
	id := db.manifest.NextSSTableID
	sstablePath := fmt.Sprintf("data-%d.sst", id)
	db.manifest.NextSSTableID++
	sst, err := sstable.CreateFromMemTable(db.memtable, sstablePath)
	if err != nil {
		return err
	}

	db.sstables = append(db.sstables, sst)

	db.manifest.SSTablePaths = append(db.manifest.SSTablePaths, sstablePath)
	if err := manifest.Write(db.manifestPath, db.manifest); err != nil {
		return err
	}

	db.memtable = memtable.New()
	db.memTableSize = 0

	if len(db.sstables) >= db.compactionThreshold {
		if err := db.Compact(); err != nil {
			log.Printf("Failed to compact SSTables: %v", err)
		}
	}

	return nil
}
