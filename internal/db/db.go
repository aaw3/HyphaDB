package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/aaw3/hyphadb/internal/blockcache"
	"github.com/aaw3/hyphadb/internal/compaction"
	"github.com/aaw3/hyphadb/internal/filelock"
	"github.com/aaw3/hyphadb/internal/fsutil"
	"github.com/aaw3/hyphadb/internal/manifest"
	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
	"github.com/aaw3/hyphadb/internal/wal"
)

type DB struct {
	dataDir             string
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
	limits              LimitsOptions
	directoryLock       *filelock.Lock

	mu               sync.RWMutex
	flushSignal      chan struct{}
	compactionSignal chan struct{}
	compactionDone   chan struct{}
	activeReaders    map[uint64]int
	closed           bool
	flushWG          sync.WaitGroup
	compactionWG     sync.WaitGroup
}

var (
	ErrClosed         = errors.New("database is closed")
	ErrDatabaseLocked = filelock.ErrLocked
	ErrKeyTooLarge    = errors.New("key exceeds configured size limit")
	ErrValueTooLarge  = errors.New("value exceeds configured size limit")
	ErrBatchTooLarge  = errors.New("batch exceeds configured size limit")
)

const (
	defaultMaxMemtableEntries  = 10_000
	defaultCompactionThreshold = 4
	defaultBlockCacheCapacity  = 64 * 1024 * 1024
	defaultMaxBatchOperations  = 10_000
	defaultMaxBatchBytes       = 128 * 1024 * 1024
)

type Options struct {
	DataDir    string
	Memtable   MemtableOptions
	Compaction CompactionOptions
	BlockCache BlockCacheOptions
	Limits     LimitsOptions
}

type MemtableOptions struct {
	// MaxEntries controls when the active memtable is rotated. Zero selects
	// the default. Negative values are invalid.
	MaxEntries int
}

type CompactionOptions struct {
	// TableCountThreshold controls when L0 compaction is scheduled. Zero
	// selects the default. Negative values are invalid.
	TableCountThreshold int
}

type BlockCacheOptions struct {
	// CapacityBytes bounds the SSTable block cache. Zero selects the default.
	// Negative values are invalid.
	CapacityBytes int
}

// LimitsOptions bounds memory used by individual writes and batches.
// Zero values select storage-safe defaults.
type LimitsOptions struct {
	MaxKeyBytes        int
	MaxValueBytes      int
	MaxBatchOperations int
	MaxBatchBytes      int
}

func New(maxMemtableSize int, compactionThreshold int) (*DB, error) {
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
	if opts.DataDir == "" {
		opts.DataDir = "."
	}
	if opts.Memtable.MaxEntries < 0 {
		return nil, fmt.Errorf("max memtable size cannot be negative")
	}
	if opts.Memtable.MaxEntries == 0 {
		opts.Memtable.MaxEntries = defaultMaxMemtableEntries
	}
	if opts.Compaction.TableCountThreshold < 0 {
		return nil, fmt.Errorf("compaction threshold cannot be negative")
	}
	if opts.Compaction.TableCountThreshold == 0 {
		opts.Compaction.TableCountThreshold = defaultCompactionThreshold
	}
	if opts.BlockCache.CapacityBytes < 0 {
		return nil, fmt.Errorf("block cache capacity cannot be negative")
	}
	if opts.BlockCache.CapacityBytes == 0 {
		opts.BlockCache.CapacityBytes = defaultBlockCacheCapacity
	}
	if err := normalizeLimits(&opts.Limits); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opts.DataDir, 0700); err != nil {
		return nil, err
	}

	directoryLock, err := filelock.Acquire(filepath.Join(opts.DataDir, "LOCK"))
	if err != nil {
		return nil, err
	}
	opened := false
	var w *wal.WAL
	defer func() {
		if opened {
			return
		}
		if w != nil {
			_ = w.Close()
		}
		_ = directoryLock.Close()
	}()

	manifestPath := filepath.Join(opts.DataDir, "MANIFEST")
	mf, err := manifest.Read(manifestPath)
	if err != nil {
		return nil, err
	}

	mt := memtable.New()

	segments, err := wal.ListSegmentsInDir(opts.DataDir)
	if err != nil {
		return nil, err
	}

	var recoveredWALMaxSeq uint64
	// Replay all WAL segments into the memtable
	// Can cause memory issues if many WAL segments exist
	// Later recovery should build multiple memtables from WAL segments if they exceed a certain size
	for _, segment := range segments {
		stats, err := wal.ReplayIntoWithStats(segment.Path, mt)
		if err != nil {
			return nil, err
		}
		if stats.MaxSequence > recoveredWALMaxSeq {
			recoveredWALMaxSeq = stats.MaxSequence
		}
	}

	// The manifest can lag behind a segment rotation if the process stops
	// before the next manifest write. Never reopen or subsequently allocate a
	// segment below one that already exists on disk.
	if len(segments) > 0 {
		lastSegmentID := segments[len(segments)-1].ID
		if lastSegmentID > mf.NextWALSegmentID {
			mf.NextWALSegmentID = lastSegmentID
		}
	}

	// open WAL for appending
	w, err = wal.NewSegmentInDir(opts.DataDir, mf.NextWALSegmentID)
	if err != nil {
		return nil, err
	}

	cache := blockcache.NewLRU(opts.BlockCache.CapacityBytes)

	database := &DB{
		dataDir:             opts.DataDir,
		memtable:            mt,
		maxMemtableSize:     opts.Memtable.MaxEntries,
		memTableSize:        mt.Len(),
		sstables:            make([]*sstable.SSTable, 0, len(mf.SSTables)),
		blockCache:          cache,
		wal:                 w,
		manifest:            mf,
		manifestPath:        manifestPath,
		compactionThreshold: opts.Compaction.TableCountThreshold,
		limits:              opts.Limits,
		directoryLock:       directoryLock,
		flushSignal:         make(chan struct{}, 1),
		compactionSignal:    make(chan struct{}, 1),
		compactionDone:      make(chan struct{}, 1),
		activeReaders:       make(map[uint64]int),
	}

	for i := range mf.SSTables {
		table := &mf.SSTables[i]
		// SizeBytes was added after the original manifest format. Recover it
		// from the file when opening an older manifest.
		if table.SizeBytes == 0 {
			if info, statErr := os.Stat(table.Path); statErr == nil {
				table.SizeBytes = uint64(info.Size())
			}
		}
		database.sstables = append(database.sstables, database.newSSTable(*table))
	}

	sstableMaxSeq, err := maxSeqFromSSTables(database.sstables)
	if err != nil {
		return nil, err
	}

	memMaxSeq := maxSeqFromMemTable(mt)

	maxSeq := max(sstableMaxSeq, memMaxSeq, recoveredWALMaxSeq)
	nextSeq := maxSeq + 1

	database.nextSeq = nextSeq

	database.flushWG.Add(1)
	go database.flushLoop()
	database.compactionWG.Add(1)
	go database.compactionLoop()

	opened = true
	return database, nil
}

func normalizeLimits(limits *LimitsOptions) error {
	if limits.MaxKeyBytes == 0 {
		limits.MaxKeyBytes = record.MaxKeySize
	}
	if limits.MaxValueBytes == 0 {
		limits.MaxValueBytes = record.MaxValueSize
	}
	if limits.MaxBatchOperations == 0 {
		limits.MaxBatchOperations = defaultMaxBatchOperations
	}
	if limits.MaxBatchBytes == 0 {
		limits.MaxBatchBytes = defaultMaxBatchBytes
	}
	if limits.MaxKeyBytes < 1 || limits.MaxKeyBytes > record.MaxKeySize {
		return fmt.Errorf("max key bytes must be between 1 and %d", record.MaxKeySize)
	}
	if limits.MaxValueBytes < 1 || limits.MaxValueBytes > record.MaxValueSize {
		return fmt.Errorf("max value bytes must be between 1 and %d", record.MaxValueSize)
	}
	if limits.MaxBatchOperations < 1 {
		return fmt.Errorf("max batch operations must be positive")
	}
	if limits.MaxBatchBytes < 1 {
		return fmt.Errorf("max batch bytes must be positive")
	}
	return nil
}

func (db *DB) validateKey(key string) error {
	if len(key) > db.limits.MaxKeyBytes {
		return fmt.Errorf(
			"%w: got %d bytes, maximum is %d",
			ErrKeyTooLarge,
			len(key),
			db.limits.MaxKeyBytes,
		)
	}
	return nil
}

func (db *DB) validateValue(value []byte) error {
	if len(value) > db.limits.MaxValueBytes {
		return fmt.Errorf(
			"%w: got %d bytes, maximum is %d",
			ErrValueTooLarge,
			len(value),
			db.limits.MaxValueBytes,
		)
	}
	return nil
}

func (db *DB) newSSTable(meta manifest.SSTableMetadata) *sstable.SSTable {
	return sstable.New(meta.Path, sstable.OpenOptions{
		ID:          meta.ID,
		Level:       meta.Level,
		SizeBytes:   meta.SizeBytes,
		SmallestKey: meta.SmallestKey,
		LargestKey:  meta.LargestKey,
		BlockCache:  db.blockCache,
	})
}

func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// An explicit compaction compacts available L0 work even when the
	// automatic threshold has not been reached.
	return db.compactLocked(1)
}

func (db *DB) compactLocked(threshold int) error {
	// Prefer draining L0 first. If no L0 work is ready, compact one L1
	// table into L2. The source levels use arithmetic so adding another
	// higher level later does not require changing the level representation.
	plan, ok := compaction.PickCompaction(
		db.manifest.SSTables,
		compaction.L0,
		threshold,
	)
	if !ok {
		plan, ok = compaction.PickCompaction(
			db.manifest.SSTables,
			compaction.L0+1,
			threshold,
		)
	}
	if !ok {
		return nil
	}

	selectedIDs := make(map[uint64]struct{}, len(plan.Inputs))
	for _, table := range plan.Inputs {
		selectedIDs[table.ID] = struct{}{}
	}

	inputTables := make([]*sstable.SSTable, 0, len(plan.Inputs))
	for _, table := range plan.Inputs {
		for _, sst := range db.sstables {
			if sst.ID == table.ID {
				inputTables = append(inputTables, sst)
				break
			}
		}
	}
	if len(inputTables) != len(plan.Inputs) {
		return fmt.Errorf("compaction plan references missing SSTable")
	}

	id := db.manifest.NextSSTableID
	compactedSSTablePath := filepath.Join(db.dataDir, fmt.Sprintf("compact-%d.sst", id))
	oldestReader, hasReader := db.oldestReaderLocked()
	var retention *uint64
	if hasReader {
		retention = &oldestReader
	}
	compactedSSTable, err := compaction.MergeSSTablesWithOptions(
		inputTables,
		compactedSSTablePath,
		compaction.MergeOptions{
			OldestReader: retention,
			DropTombstones: plan.TargetLevel ==
				compaction.HighestSupportedLevel,
		},
	)
	if err != nil {
		return err
	}

	// write compacted SSTable to MANIFEST file

	oldNextSSTableID := db.manifest.NextSSTableID
	oldTables := db.manifest.SSTables
	db.manifest.NextSSTableID++
	compactedMeta := manifest.SSTableMetadata{
		ID:          id,
		Path:        compactedSSTablePath,
		Level:       plan.TargetLevel,
		SizeBytes:   compactedSSTable.SizeBytes,
		SmallestKey: compactedSSTable.SmallestKey,
		LargestKey:  compactedSSTable.LargestKey,
	}
	newMetadata := make([]manifest.SSTableMetadata, 0,
		len(db.manifest.SSTables)-len(plan.Inputs)+1,
	)
	inserted := false
	for _, table := range db.manifest.SSTables {
		if _, selected := selectedIDs[table.ID]; selected {
			if !inserted {
				newMetadata = append(newMetadata, compactedMeta)
				inserted = true
			}
			continue
		}
		newMetadata = append(newMetadata, table)
	}
	db.manifest.SSTables = newMetadata

	manifestErr := manifest.Write(db.manifestPath, db.manifest)
	if manifestErr != nil && !manifest.IsPublished(manifestErr) {
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
		return manifestErr
	}

	newSSTables := make([]*sstable.SSTable, 0,
		len(db.sstables)-len(inputTables)+1,
	)
	inserted = false
	for _, sst := range db.sstables {
		if _, selected := selectedIDs[sst.ID]; selected {
			if !inserted {
				compactedSSTable = db.newSSTable(compactedMeta)
				newSSTables = append(newSSTables, compactedSSTable)
				inserted = true
			}
			continue
		}
		newSSTables = append(newSSTables, sst)
	}
	oldSSTables := db.sstables
	db.sstables = newSSTables
	if manifestErr != nil {
		// The rename is visible, so retain the newly published state. Keep the
		// old files as a safe fallback because directory durability is unknown.
		return manifestErr
	}

	removedOldTable := false
	var cleanupErr error
	for _, sst := range oldSSTables {
		if _, selected := selectedIDs[sst.ID]; !selected {
			continue
		}
		if err := os.Remove(sst.Path); err != nil && !os.IsNotExist(err) {
			log.Printf("failed while deleting old SSTable %s: %v",
				sst.Path,
				err,
			)
			cleanupErr = errors.Join(cleanupErr, err)
		} else if err == nil {
			removedOldTable = true
		}
		db.blockCache.PurgeTable(sst.ID)
	}
	if removedOldTable {
		if err := fsutil.SyncDir(db.dataDir); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}

	return cleanupErr
}

func (db *DB) Get(key string) ([]byte, error) {
	// Holds the read lock during SSTable access so compaction doesn't delete
	// a table while this lookup is opening / reading it
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, ErrClosed
	}
	if err := db.validateKey(key); err != nil {
		return nil, err
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

func (db *DB) registerReaderLocked(sequence uint64) {
	db.activeReaders[sequence]++
}

func (db *DB) unregisterReaderLocked(sequence uint64) {
	count := db.activeReaders[sequence]
	if count <= 1 {
		delete(db.activeReaders, sequence)
		return
	}
	db.activeReaders[sequence] = count - 1
}

func (db *DB) oldestReaderLocked() (uint64, bool) {
	var oldest uint64
	found := false

	for sequence := range db.activeReaders {
		if !found || sequence < oldest {
			oldest = sequence
			found = true
		}
	}

	return oldest, found
}

func hasCompactionWork(tables []manifest.SSTableMetadata, threshold int) bool {
	if _, ok := compaction.PickCompaction(tables, compaction.L0, threshold); ok {
		return true
	}
	_, ok := compaction.PickCompaction(tables, compaction.L0+1, threshold)
	return ok
}

// currentSequenceLocked returns the boundary before the next sequence that
// may be allocated. Open initializes nextSeq from persisted state and every
// successful mutation advances it while holding db.mu, so readers do not need
// to rescan memtables or SSTables. The caller must hold db.mu.
func (db *DB) currentSequenceLocked() uint64 {
	return db.nextSeq - 1
}

func (db *DB) Put(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrClosed
	}
	if err := db.validateKey(key); err != nil {
		return err
	}
	if err := db.validateValue(value); err != nil {
		return err
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
	if err := db.validateKey(key); err != nil {
		return err
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

	newWAL, err := wal.NewSegmentInDir(db.dataDir, db.manifest.NextWALSegmentID)
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

func (db *DB) signalCompaction() {
	select {
	case db.compactionSignal <- struct{}{}:
	default:
	}
}

func (db *DB) flushLoop() {
	defer db.flushWG.Done()

	for range db.flushSignal {
		db.flushUntilEmpty()
	}

	// run one last flush after the channel closed
	db.flushUntilEmpty()
	db.signalCompaction()
}

func (db *DB) compactionLoop() {
	defer db.compactionWG.Done()

	for range db.compactionSignal {
		db.mu.Lock()
		for hasCompactionWork(db.manifest.SSTables, db.compactionThreshold) {
			if err := db.compactLocked(db.compactionThreshold); err != nil {
				log.Printf("Failed to compact SSTables: %v", err)
				break
			}
		}
		db.mu.Unlock()

		select {
		case db.compactionDone <- struct{}{}:
		default:
		}
	}
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
	sstablePath := filepath.Join(db.dataDir, fmt.Sprintf("data-%d.sst", id))
	oldNextSSTableID := db.manifest.NextSSTableID
	db.manifest.NextSSTableID++
	db.mu.Unlock()

	createdSSTable, err := sstable.CreateFromMemTable(imm.MemTable, sstablePath)
	if err != nil {
		db.mu.Lock()
		db.manifest.NextSSTableID = oldNextSSTableID
		db.mu.Unlock()
		return err
	}

	meta := manifest.SSTableMetadata{
		ID:          id,
		Path:        sstablePath,
		Level:       0,
		SizeBytes:   createdSSTable.SizeBytes,
		SmallestKey: createdSSTable.SmallestKey,
		LargestKey:  createdSSTable.LargestKey,
	}
	sst := db.newSSTable(meta)

	db.mu.Lock()
	oldSSTableCount := len(db.sstables)
	oldMetadataCount := len(db.manifest.SSTables)

	db.sstables = append(db.sstables, sst)
	db.manifest.SSTables = append(db.manifest.SSTables, meta)

	manifestErr := manifest.Write(db.manifestPath, db.manifest)
	if manifestErr != nil && !manifest.IsPublished(manifestErr) {
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

		return manifestErr
	}

	if len(db.immutableMemtables) > 0 && db.immutableMemtables[0] == imm {
		// remove flushed immutable memtable from the list
		db.immutableMemtables = db.immutableMemtables[1:]
	}

	db.mu.Unlock()
	if manifestErr != nil {
		// Keep the WAL when directory durability is uncertain. Recovery may
		// replay duplicate sequence numbers, which is safe and preferable to
		// losing the only durable copy.
		return manifestErr
	}

	if err := wal.RemoveSegmentInDir(db.dataDir, imm.WalID); err != nil {
		return err
	}

	// Compaction runs independently from flushing. It remains serialized by
	// the compaction worker until the merge phase is made lock-free.
	db.signalCompaction()

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
	close(db.compactionSignal)
	db.compactionWG.Wait()

	var walErr error
	if db.wal != nil {
		walErr = db.wal.Close()
	}
	return errors.Join(walErr, db.directoryLock.Close())
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
