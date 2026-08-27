package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aaw3/hyphadb/internal/blockcache"
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
	blockCache          blockcache.Cache
	wal                 *wal.WAL
	manifest            *manifest.Manifest
	manifestPath        string
	compactionThreshold int
	nextSeq             uint64

	mu          sync.RWMutex
	flushSignal chan struct{}
	closed      bool
	flushWG     sync.WaitGroup
}

var ErrClosed = errors.New("database is closed")

const defaultBlockCacheCapacity = 64 * 1024 * 1024

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

	cache := blockcache.NewLRU(defaultBlockCacheCapacity)

	database := &DB{
		memtable:            mt,
		maxMemtableSize:     maxMemtableSize,
		memTableSize:        mt.Len(),
		sstables:            make([]*sstable.SSTable, 0, len(mf.SSTables)),
		blockCache:          cache,
		wal:                 w,
		manifest:            mf,
		manifestPath:        manifestPath,
		compactionThreshold: compactionThreshold,
		flushSignal:         make(chan struct{}, 1),
	}

	for _, table := range mf.SSTables {
		database.sstables = append(database.sstables, database.newSSTable(table))
	}

	sstableMaxSeq, err := maxSeqFromSSTables(database.sstables)
	if err != nil {
		return nil, err
	}

	memMaxSeq := maxSeqFromMemTable(mt)

	maxSeq := max(sstableMaxSeq, memMaxSeq)
	nextSeq := maxSeq + 1

	database.nextSeq = nextSeq

	database.flushWG.Add(1)
	go database.flushLoop()

	return database, nil
}

func (db *DB) newSSTable(meta manifest.SSTableMetadata) *sstable.SSTable {
	return sstable.New(meta.Path, sstable.OpenOptions{
		ID:         meta.ID,
		BlockCache: db.blockCache,
	})
}

func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.compactLocked()
}

func (db *DB) compactLocked() error {
	id := db.manifest.NextSSTableID
	compactedSSTablePath := fmt.Sprintf("compact-%d.sst", id)
	_, err := compaction.MergeSSTables(db.sstables, compactedSSTablePath)
	if err != nil {
		return err
	}

	// write compacted SSTable to MANIFEST file

	oldNextSSTableID := db.manifest.NextSSTableID
	oldTables := db.manifest.SSTables
	db.manifest.NextSSTableID++
	compactedMeta := manifest.SSTableMetadata{
		ID:   id,
		Path: compactedSSTablePath,
	}
	db.manifest.SSTables = []manifest.SSTableMetadata{
		compactedMeta,
	}

	if err := manifest.Write(db.manifestPath, db.manifest); err != nil {
		// Restore in-memory manifest since persistence failed
		db.manifest.NextSSTableID = oldNextSSTableID
		db.manifest.SSTables = oldTables

		if removeErr := os.Remove(compactedSSTablePath); removeErr != nil &&
			!os.IsNotExist(removeErr) {
			log.Printf(
				"failed to clean up orphaned compacted SStable %s: %v",
				compactedSSTablePath,
				removeErr,
			)
		}
		return err
	}

	oldSSTables := db.sstables
	compactedSSTable := db.newSSTable(compactedMeta)
	db.sstables = []*sstable.SSTable{compactedSSTable}

	for _, sst := range oldSSTables {
		if err := os.Remove(sst.Path); err != nil {
			log.Printf("failed while deleting old SSTable %s: %v",
				sst.Path,
				err,
			)
		}
		db.blockCache.PurgeTable(sst.ID)
	}

	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	// Holds the read lock during SSTable access so compaction doesn't delete
	// a table while this lookup is opening / reading it
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}

	return db.getAt(key, ^uint64(0))
}

func (db *DB) getAt(key string, maxSeq uint64) ([]byte, error) {
	var best record.Record
	var found bool

	consider := func(rec record.Record, exists bool) {
		if exists && (!found || rec.Seq > best.Seq) {
			best = rec
			found = true
		}
	}

	consider(db.memtable.GetAt(key, maxSeq))
	for i := len(db.immutableMemtables) - 1; i >= 0; i-- {
		consider(db.immutableMemtables[i].MemTable.GetAt(key, maxSeq))
	}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		rec, exists, err := db.sstables[i].GetRecordAt(key, maxSeq)
		if err != nil {
			return nil, err
		}
		consider(rec, exists)
	}

	if !found || best.Deleted {
		return nil, sstable.ErrNotFound
	}
	return best.Value, nil
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
	oldNextSSTableID := db.manifest.NextSSTableID
	db.manifest.NextSSTableID++
	db.mu.Unlock()

	_, err := sstable.CreateFromMemTable(imm.MemTable, sstablePath)
	if err != nil {
		db.mu.Lock()
		db.manifest.NextSSTableID = oldNextSSTableID
		db.mu.Unlock()
		return err
	}

	meta := manifest.SSTableMetadata{
		ID:   id,
		Path: sstablePath,
	}
	sst := db.newSSTable(meta)

	db.mu.Lock()
	oldSSTableCount := len(db.sstables)
	oldMetadataCount := len(db.manifest.SSTables)

	db.sstables = append(db.sstables, sst)
	db.manifest.SSTables = append(db.manifest.SSTables, meta)

	if err := manifest.Write(db.manifestPath, db.manifest); err != nil {
		db.sstables = db.sstables[:oldSSTableCount]
		db.manifest.SSTables = db.manifest.SSTables[:oldMetadataCount]
		db.manifest.NextSSTableID = oldNextSSTableID
		db.mu.Unlock()

		if removeErr := os.Remove(sstablePath); removeErr != nil &&
			!os.IsNotExist(removeErr) {
			log.Printf(
				"failed to remove orphaned SSTable %s: %v",
				sstablePath,
				removeErr,
			)
		}

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
		if err := db.compactLocked(); err != nil {
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

	it := mt.Iterator()
	defer it.Close()

	for it.Next() {
		rec := it.Record()
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
