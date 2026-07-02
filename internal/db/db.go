package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aaw3/hyphadb/internal/compaction"
	"github.com/aaw3/hyphadb/internal/manifest"
	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
	"github.com/aaw3/hyphadb/internal/wal"
)

type DB struct {
	memtable            *memtable.MemTable
	immutableMemtables  []*memtable.ImmutableMemTable
	maxMemtableSize     int
	memTableSize        int
	sstables            []*sstable.SSTable
	wal                 *wal.WAL
	manifest            *manifest.Manifest
	manifestPath        string
	compactionThreshold int
	nextSeq             uint64

	mu          sync.Mutex
	flushSignal chan struct{}
	closed      bool
	flushWG     sync.WaitGroup
}

var ErrClosed = errors.New("database is closed")

func New(maxMemtableSize int, compactionThreshold int) (*DB, error) {
	manifestPath := "MANIFEST"
	mf, err := manifest.Read(manifestPath)
	if err != nil {
		return nil, err
	}

	mt := memtable.New()

	segments, err := wal.ListSegments()
	if err != nil {
		return nil, err
	}

	// Replay all WAL segments into the memtable
	// Can cause memory issues if many WAL segments exist
	// Later recovery should build multiple memtables from WAL segments if they exceed a certain size
	for _, segment := range segments {
		if err := wal.ReplayInto(segment.Path, mt); err != nil {
			return nil, err
		}
	}

	// open WAL for appending
	w, err := wal.NewSegment(mf.NextWALSegmentID)
	if err != nil {
		return nil, err
	}

	sstables := make([]*sstable.SSTable, 0, len(mf.SSTablePaths))
	for _, path := range mf.SSTablePaths {
		sstables = append(sstables, &sstable.SSTable{Path: path})
	}

	sstableMaxSeq, err := maxSeqFromSSTables(sstables)
	if err != nil {
		return nil, err
	}

	memMaxSeq := maxSeqFromMemTable(mt)

	maxSeq := max(sstableMaxSeq, memMaxSeq)
	nextSeq := maxSeq + 1

	database := &DB{
		memtable:            mt,
		maxMemtableSize:     maxMemtableSize,
		memTableSize:        len(mt.Records()),
		sstables:            sstables,
		wal:                 w,
		manifest:            mf,
		manifestPath:        manifestPath,
		compactionThreshold: compactionThreshold,
		nextSeq:             nextSeq,
		flushSignal:         make(chan struct{}, 1),
	}

	database.flushWG.Add(1)
	go database.flushLoop()

	return database, nil
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
	// Locks during search, should be replaced with rwmutex and sstable snapshot to allow concurrent reads
	db.mu.Lock()
	defer db.mu.Unlock()

	if rec, exists := db.memtable.Get(key); exists {
		if rec.Deleted {
			return nil, sstable.ErrNotFound
		}

		return rec.Value, nil
	}

	for i := len(db.immutableMemtables) - 1; i >= 0; i-- {
		if rec, exists := db.immutableMemtables[i].MemTable.Get(key); exists {
			if rec.Deleted {
				return nil, sstable.ErrNotFound
			}

			return rec.Value, nil
		}
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
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	seq := db.nextSeq

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

	db.nextSeq++
	db.memtable.Put(rec)
	db.memTableSize++

	if db.memTableSize >= db.maxMemtableSize {
		return db.rotateMemtable()
	}
	return nil
}

func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}

	seq := db.nextSeq

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

	db.nextSeq++
	db.memtable.Put(rec)
	db.memTableSize++

	if db.memTableSize >= db.maxMemtableSize {
		return db.rotateMemtable()
	}

	return nil
}

func (db *DB) rotateMemtable() error {
	oldWAL := db.wal
	db.immutableMemtables = append(db.immutableMemtables, &memtable.ImmutableMemTable{
		MemTable: db.memtable,
		WalID:    oldWAL.ID,
	})

	db.manifest.NextWALSegmentID++

	newWAL, err := wal.NewSegment(db.manifest.NextWALSegmentID)
	if err != nil {
		return err
	}

	db.memtable = memtable.New()
	db.memTableSize = 0
	db.wal = newWAL

	if err := oldWAL.Close(); err != nil {
		return err
	}

	db.signalFlush()
	return nil
}

// send an event to the flushLoop
func (db *DB) signalFlush() {
	// non-blocking send to flushSignal channel
	select {
	// send 0-length struct as signal
	case db.flushSignal <- struct{}{}:
	default:
		// do nothing on channel full
	}
}

func (db *DB) flushLoop() {
	defer db.flushWG.Done()

	for range db.flushSignal {
		db.flushUntilEmpty()
	}

	// run one last flush after the channel closed
	db.flushUntilEmpty()
}

func (db *DB) flushUntilEmpty() {
	for {
		db.mu.Lock()

		if len(db.immutableMemtables) == 0 {
			db.mu.Unlock()
			return
		}

		// flush oldest immutable memtable
		imm := db.immutableMemtables[0]
		db.mu.Unlock()

		if err := db.flushImmutableMemtable(imm); err != nil {
			log.Printf("Failed to flush immutable memtable: %v", err)
			return
		}
	}
}

func (db *DB) flushImmutableMemtable(imm *memtable.ImmutableMemTable) error {
	if imm == nil {
		return nil
	}

	// lock throughout the function to ensure that the sstables and manifest are updated atomically

	db.mu.Lock()
	id := db.manifest.NextSSTableID
	sstablePath := fmt.Sprintf("data-%d.sst", id)
	db.manifest.NextSSTableID++
	db.mu.Unlock()

	sst, err := sstable.CreateFromMemTable(imm.MemTable, sstablePath)
	if err != nil {
		return err
	}

	db.mu.Lock()
	db.sstables = append(db.sstables, sst)
	db.manifest.SSTablePaths = append(db.manifest.SSTablePaths, sstablePath)

	if err := manifest.Write(db.manifestPath, db.manifest); err != nil {
		db.mu.Unlock()
		return err
	}

	if len(db.immutableMemtables) > 0 && db.immutableMemtables[0] == imm {
		// remove flushed immutable memtable from the list
		db.immutableMemtables = db.immutableMemtables[1:]
	}

	shouldCompact := len(db.sstables) >= db.compactionThreshold
	db.mu.Unlock()

	if err := wal.RemoveSegment(imm.WalID); err != nil {
		return err
	}

	// Compact synchronously for now since it mutates sstables and manifest.
	db.mu.Lock()
	if shouldCompact {
		if err := db.Compact(); err != nil {
			log.Printf("Failed to compact SSTables: %v", err)
		}
	}
	db.mu.Unlock()

	return nil
}

// Close database, ensure immutable memtables flush to disk and close active WAL
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}

	db.closed = true
	close(db.flushSignal)
	db.mu.Unlock()

	db.flushWG.Wait()

	if db.wal != nil {
		return db.wal.Close()
	}

	return nil
}

func maxSeqFromMemTable(mt *memtable.MemTable) uint64 {
	var maxSeq uint64

	for _, rec := range mt.Records() {
		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
	}

	return maxSeq
}

func maxSeqFromSSTables(sstables []*sstable.SSTable) (uint64, error) {
	var maxSeq uint64

	for _, sst := range sstables {
		seq, err := sst.MaxSeq()
		if err != nil {
			return 0, err
		}

		if seq > maxSeq {
			maxSeq = seq
		}
	}

	return maxSeq, nil
}
