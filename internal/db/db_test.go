package db

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/aaw3/hyphadb/internal/blockcache"
	"github.com/aaw3/hyphadb/internal/compaction"
	"github.com/aaw3/hyphadb/internal/manifest"
	"github.com/aaw3/hyphadb/internal/memtable"
	"github.com/aaw3/hyphadb/internal/record"
	"github.com/aaw3/hyphadb/internal/sstable"
)

func useTempWorkingDirectory(t *testing.T) {
	t.Helper()

	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd error: %v", err)
	}

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("Chdir temp directory failed: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Chdir(oldDir); err != nil {
			t.Errorf("restore working directory failed: %v", err)
		}
	})
}

func dbLRUCache(t *testing.T, database *DB) *blockcache.LRU {
	t.Helper()

	cache, ok := database.blockCache.(*blockcache.LRU)
	if !ok {
		t.Fatalf("blockCache = %T, want *blockcache.LRU", database.blockCache)
	}

	return cache
}

func TestFlushDeletesWALAndRestartReadsFromSSTable(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(2, 10)
	if err != nil {
		t.Fatalf("new db: %v", err)
	}

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("put apple: %v", err)
	}
	if err := database.Put("banana", []byte("yellow")); err != nil {
		t.Fatalf("put banana: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	// wal gets flushed to sstable
	if _, err := os.Stat("wal-0.log"); !os.IsNotExist(err) {
		t.Fatalf("wal-0.log should be deleted after flush, got err=%v", err)
	}

	// wal-1.log replaces wal-0.log after flush
	if _, err := os.Stat("wal-1.log"); err != nil {
		t.Fatalf("expected active wal-1.log to exist: %v", err)
	}

	// sstable created after flush
	if _, err := os.Stat("data-0.sst"); err != nil {
		t.Fatalf("expected data-0.sst to exist: %v", err)
	}

	if len(database.manifest.SSTables) != 1 {
		t.Fatalf("manifest SSTables = %v, want one table", database.manifest.SSTables)
	}

	if database.manifest.SSTables[0].ID != 0 ||
		database.manifest.SSTables[0].Path != "data-0.sst" {
		t.Fatalf(
			"manifest SSTable metadata = %+v, want {ID:0 Path:data-0.sst}",
			database.manifest.SSTables[0],
		)
	}

	// reopen the database and check saved values
	reopened, err := New(2, 10)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	if len(reopened.sstables) != 1 {
		t.Fatalf("reopened SSTables = %d, want 1", len(reopened.sstables))
	}

	if reopened.sstables[0].ID != 0 || reopened.sstables[0].Path != "data-0.sst" {
		t.Fatalf(
			"reopened SSTable metadata = {ID:%d Path:%s}, want {ID:0 Path:data-0.sst}",
			reopened.sstables[0].ID,
			reopened.sstables[0].Path,
		)
	}

	got, err := reopened.Get("apple")
	if err != nil {
		t.Fatalf("get apple after restart: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("get apple = %q, want red", got)
	}

	got, err = reopened.Get("banana")
	if err != nil {
		t.Fatalf("get banana after restart: %v", err)
	}
	if string(got) != "yellow" {
		t.Fatalf("get banana = %q, want yellow", got)
	}
}

func TestOpenUsesConfiguredDataDirectory(t *testing.T) {
	useTempWorkingDirectory(t)
	dataDir := filepath.Join(t.TempDir(), "hyphadb")

	database, err := Open(Options{
		DataDir:             dataDir,
		MaxMemtableSize:     2,
		CompactionThreshold: 10,
		BlockCacheCapacity:  1024,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := database.Put("banana", []byte("yellow")); err != nil {
		t.Fatalf("Put banana: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dataDir, "MANIFEST")); err != nil {
		t.Fatalf("configured MANIFEST: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "data-0.sst")); err != nil {
		t.Fatalf("configured SSTable: %v", err)
	}

	reopened, err := Open(Options{
		DataDir:             dataDir,
		MaxMemtableSize:     2,
		CompactionThreshold: 10,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.Get("apple")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("apple = %q, want red", got)
	}
}

func TestConcurrentReadersOfActiveMemtable(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(10_000, 100)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer database.Close()

	if err := database.Put("stable", []byte("value")); err != nil {
		t.Fatalf("Put error: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 16; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 500; j++ {
				got, err := database.Get("stable")
				if err != nil {
					t.Errorf("Get error: %v", err)
					return
				}

				if string(got) != "value" {
					t.Errorf("value = %q, want value", got)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestCompactionPersistsSSTableMetadata(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(2, 2)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put apple: %v", err)
	}
	if err := database.Put("banana", []byte("yellow")); err != nil {
		t.Fatalf("Put banana: %v", err)
	}
	if err := database.Put("carrot", []byte("orange")); err != nil {
		t.Fatalf("Put carrot: %v", err)
	}
	if err := database.Put("date", []byte("brown")); err != nil {
		t.Fatalf("Put date: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if len(database.manifest.SSTables) != 1 {
		t.Fatalf("manifest SSTables = %v, want one compacted table", database.manifest.SSTables)
	}

	table := database.manifest.SSTables[0]
	if table.ID != 2 || table.Path != "compact-2.sst" {
		t.Fatalf(
			"compacted SSTable metadata = %+v, want {ID:2 Path:compact-2.sst}",
			table,
		)
	}
	if table.SizeBytes == 0 {
		t.Fatal("compacted SSTable metadata has zero SizeBytes")
	}

	reopened, err := New(2, 2)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.Get("carrot")
	if err != nil {
		t.Fatalf("Get carrot after restart: %v", err)
	}

	if string(got) != "orange" {
		t.Fatalf("carrot = %q, want orange", got)
	}
}

func TestReopenRecoversMissingSSTableSizeMetadata(t *testing.T) {
	useTempWorkingDirectory(t)

	_, err := sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	}, "data-0.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create SSTable: %v", err)
	}

	stored := &manifest.Manifest{
		NextSSTableID:    1,
		NextWALSegmentID: 0,
		SSTables: []manifest.SSTableMetadata{{
			ID:   0,
			Path: "data-0.sst",
			// SizeBytes intentionally remains zero to represent an older
			// manifest written before size metadata existed.
		}},
	}
	if err := manifest.Write("MANIFEST", stored); err != nil {
		t.Fatalf("write legacy manifest: %v", err)
	}

	database, err := New(100, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	info, err := os.Stat("data-0.sst")
	if err != nil {
		t.Fatalf("stat SSTable: %v", err)
	}
	got := database.manifest.SSTables[0].SizeBytes
	if got != uint64(info.Size()) {
		t.Fatalf("recovered SizeBytes = %d, want %d", got, info.Size())
	}
}

func TestRecoveredSSTablesUseDBBlockCache(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(2, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put apple: %v", err)
	}
	if err := database.Put("banana", []byte("yellow")); err != nil {
		t.Fatalf("Put banana: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := New(2, 10)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	cache := dbLRUCache(t, reopened)
	cache.Delete(blockcache.Key{TableID: 0, Offset: 0})

	got, err := reopened.Get("apple")
	if err != nil {
		t.Fatalf("Get apple: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("apple = %q, want red", got)
	}

	if _, ok := cache.Get(blockcache.Key{TableID: 0, Offset: 0}); !ok {
		t.Fatal("expected recovered SSTable read to populate DB block cache")
	}
}

func TestFlushedSSTablesUseDBBlockCache(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put apple: %v", err)
	}

	imm := &memtable.ImmutableMemTable{
		MemTable: database.memtable,
		WalID:    database.wal.ID,
	}
	database.immutableMemtables = append(database.immutableMemtables, imm)
	database.memtable = memtable.New()
	database.memTableSize = 0

	if err := database.flushImmutableMemtable(imm); err != nil {
		t.Fatalf("flushImmutableMemtable: %v", err)
	}

	got, err := database.Get("apple")
	if err != nil {
		t.Fatalf("Get apple: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("apple = %q, want red", got)
	}

	cache := dbLRUCache(t, database)
	if _, ok := cache.Get(blockcache.Key{TableID: 0, Offset: 0}); !ok {
		t.Fatal("expected flushed SSTable read to populate DB block cache")
	}
}

func TestDBBlockCacheServesCachedBlockWhenFileUnavailable(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	_, err = sstable.CreateFromRecords([]record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
	}, "data-0.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create SSTable: %v", err)
	}

	database.sstables = []*sstable.SSTable{
		database.newSSTable(manifest.SSTableMetadata{
			ID:   0,
			Path: "data-0.sst",
		}),
	}

	got, err := database.Get("apple")
	if err != nil {
		t.Fatalf("first Get apple: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("first apple = %q, want red", got)
	}

	if err := os.Rename("data-0.sst", "data-0.sst.hidden"); err != nil {
		t.Fatalf("rename SSTable: %v", err)
	}

	got, err = database.Get("apple")
	if err != nil {
		t.Fatalf("second Get apple from cache: %v", err)
	}
	if string(got) != "red" {
		t.Fatalf("second apple = %q, want red", got)
	}
}

func TestDBBlockCacheSeparatesTablesByID(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	_, err = sstable.CreateFromRecords([]record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
	}, "data-0.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create first SSTable: %v", err)
	}

	_, err = sstable.CreateFromRecords([]record.Record{
		{
			Key: "banana",
			Seq: 2,
			Entry: record.Entry{
				Value: []byte("yellow"),
			},
		},
	}, "data-1.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create second SSTable: %v", err)
	}

	database.sstables = []*sstable.SSTable{
		database.newSSTable(manifest.SSTableMetadata{
			ID:   0,
			Path: "data-0.sst",
		}),
		database.newSSTable(manifest.SSTableMetadata{
			ID:   1,
			Path: "data-1.sst",
		}),
	}

	if _, err := database.Get("apple"); err != nil {
		t.Fatalf("Get apple: %v", err)
	}
	if _, err := database.Get("banana"); err != nil {
		t.Fatalf("Get banana: %v", err)
	}

	cache := dbLRUCache(t, database)
	if _, ok := cache.Get(blockcache.Key{TableID: 0, Offset: 0}); !ok {
		t.Fatal("expected table 0 offset 0 cache entry")
	}
	if _, ok := cache.Get(blockcache.Key{TableID: 1, Offset: 0}); !ok {
		t.Fatal("expected table 1 offset 0 cache entry")
	}
	if cache.Len() != 2 {
		t.Fatalf("cache Len = %d, want 2", cache.Len())
	}
}

func TestCompactionPurgesOldTableCacheEntries(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	_, err = sstable.CreateFromRecords([]record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
	}, "data-0.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create first SSTable: %v", err)
	}

	_, err = sstable.CreateFromRecords([]record.Record{
		{
			Key: "banana",
			Seq: 2,
			Entry: record.Entry{
				Value: []byte("yellow"),
			},
		},
	}, "data-1.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create second SSTable: %v", err)
	}

	database.sstables = []*sstable.SSTable{
		database.newSSTable(manifest.SSTableMetadata{
			ID:   0,
			Path: "data-0.sst",
		}),
		database.newSSTable(manifest.SSTableMetadata{
			ID:   1,
			Path: "data-1.sst",
		}),
	}
	database.manifest.NextSSTableID = 2
	database.manifest.SSTables = []manifest.SSTableMetadata{
		{
			ID:   0,
			Path: "data-0.sst",
		},
		{
			ID:   1,
			Path: "data-1.sst",
		},
	}

	if _, err := database.Get("apple"); err != nil {
		t.Fatalf("Get apple: %v", err)
	}
	if _, err := database.Get("banana"); err != nil {
		t.Fatalf("Get banana: %v", err)
	}

	cache := dbLRUCache(t, database)
	if _, ok := cache.Get(blockcache.Key{TableID: 0, Offset: 0}); !ok {
		t.Fatal("expected table 0 cache entry before compaction")
	}
	if _, ok := cache.Get(blockcache.Key{TableID: 1, Offset: 0}); !ok {
		t.Fatal("expected table 1 cache entry before compaction")
	}

	if err := database.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if _, ok := cache.Get(blockcache.Key{TableID: 0, Offset: 0}); ok {
		t.Fatal("expected table 0 cache entry to be purged")
	}
	if _, ok := cache.Get(blockcache.Key{TableID: 1, Offset: 0}); ok {
		t.Fatal("expected table 1 cache entry to be purged")
	}

	if len(database.sstables) != 1 || database.sstables[0].ID != 2 {
		t.Fatalf("compacted SSTables = %+v, want one table with ID 2", database.sstables)
	}

	got, err := database.Get("banana")
	if err != nil {
		t.Fatalf("Get banana after compaction: %v", err)
	}
	if string(got) != "yellow" {
		t.Fatalf("banana = %q, want yellow", got)
	}

	if _, ok := cache.Get(blockcache.Key{TableID: 2, Offset: 0}); !ok {
		t.Fatal("expected compacted table cache entry")
	}
}

func TestCompactionPreservesDisjointL1Table(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	_, err = sstable.CreateFromRecords([]record.Record{
		{Key: "apple", Seq: 1, Entry: record.Entry{Value: []byte("red")}},
	}, "data-0.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create L0 SSTable: %v", err)
	}
	_, err = sstable.CreateFromRecords([]record.Record{
		{Key: "zebra", Seq: 2, Entry: record.Entry{Value: []byte("black-white")}},
	}, "data-1.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create L1 SSTable: %v", err)
	}

	database.sstables = []*sstable.SSTable{
		database.newSSTable(manifest.SSTableMetadata{
			ID: 0, Path: "data-0.sst", Level: compaction.L0,
			SmallestKey: "apple", LargestKey: "apple",
		}),
		database.newSSTable(manifest.SSTableMetadata{
			ID: 1, Path: "data-1.sst", Level: compaction.L0 + 1,
			SmallestKey: "zebra", LargestKey: "zebra",
		}),
	}
	database.manifest.NextSSTableID = 2
	database.manifest.SSTables = []manifest.SSTableMetadata{
		{ID: 0, Path: "data-0.sst", Level: compaction.L0, SmallestKey: "apple", LargestKey: "apple"},
		{ID: 1, Path: "data-1.sst", Level: compaction.L0 + 1, SmallestKey: "zebra", LargestKey: "zebra"},
	}

	if err := database.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if len(database.sstables) != 2 {
		t.Fatalf("SSTables after compaction = %d, want 2", len(database.sstables))
	}
	if database.sstables[0].Level != compaction.L0+1 {
		t.Fatalf("compacted table level = %d, want 1", database.sstables[0].Level)
	}
	if database.sstables[1].ID != 1 || database.sstables[1].Level != compaction.L0+1 {
		t.Fatalf("disjoint table = %+v, want ID 1 at level 1", database.sstables[1])
	}

	if _, err := database.Get("apple"); err != nil {
		t.Fatalf("Get apple: %v", err)
	}
	if _, err := database.Get("zebra"); err != nil {
		t.Fatalf("Get zebra: %v", err)
	}
	if _, err := os.Stat("data-1.sst"); err != nil {
		t.Fatalf("disjoint L1 table was removed: %v", err)
	}
}

func TestCompactionMergesL1IntoL2(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 10)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	createTable := func(id uint64, level uint32, path, key, value string) {
		t.Helper()
		_, err := sstable.CreateFromRecords([]record.Record{{
			Key:   key,
			Seq:   id + 1,
			Entry: record.Entry{Value: []byte(value)},
		}}, path, sstable.DefaultBlockSize)
		if err != nil {
			t.Fatalf("create SSTable %d: %v", id, err)
		}

		meta := manifest.SSTableMetadata{
			ID:          id,
			Path:        path,
			Level:       level,
			SmallestKey: key,
			LargestKey:  key,
		}
		database.sstables = append(database.sstables, database.newSSTable(meta))
		database.manifest.SSTables = append(database.manifest.SSTables, meta)
	}

	createTable(0, compaction.L0+1, "data-0.sst", "apple", "red")
	createTable(1, compaction.L0+2, "data-1.sst", "apple", "green")
	createTable(2, compaction.L0+2, "zebra", "zebra", "black-white")
	database.manifest.NextSSTableID = 3

	if err := database.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if len(database.sstables) != 2 {
		t.Fatalf("SSTables after compaction = %d, want 2", len(database.sstables))
	}
	if database.sstables[0].ID != 3 || database.sstables[0].Level != compaction.L0+2 {
		t.Fatalf("compacted table = %+v, want ID 3 at level 2", database.sstables[0])
	}
	if database.sstables[1].ID != 2 || database.sstables[1].Level != compaction.L0+2 {
		t.Fatalf("disjoint L2 table = %+v, want ID 2 at level 2", database.sstables[1])
	}

	got, err := database.Get("apple")
	if err != nil {
		t.Fatalf("Get apple: %v", err)
	}
	if string(got) != "green" {
		t.Fatalf("apple = %q, want green", got)
	}
	if _, err := os.Stat("data-0.sst"); !os.IsNotExist(err) {
		t.Fatalf("old L1 SSTable was not removed: %v", err)
	}
	if _, err := os.Stat("data-1.sst"); !os.IsNotExist(err) {
		t.Fatalf("overlapping L2 SSTable was not removed: %v", err)
	}
}

func TestFlushAutomaticallyCompactsEligibleL1Tables(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 2)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	createTable := func(id uint64, level uint32, path, key, value string) {
		t.Helper()
		_, err := sstable.CreateFromRecords([]record.Record{{
			Key:   key,
			Seq:   id + 1,
			Entry: record.Entry{Value: []byte(value)},
		}}, path, sstable.DefaultBlockSize)
		if err != nil {
			t.Fatalf("create SSTable %d: %v", id, err)
		}

		meta := manifest.SSTableMetadata{
			ID:          id,
			Path:        path,
			Level:       level,
			SmallestKey: key,
			LargestKey:  key,
		}
		database.sstables = append(database.sstables, database.newSSTable(meta))
		database.manifest.SSTables = append(database.manifest.SSTables, meta)
	}

	createTable(0, compaction.L0+1, "data-0.sst", "apple", "red")
	createTable(1, compaction.L0+1, "data-1.sst", "banana", "yellow")
	createTable(2, compaction.L0+2, "data-2.sst", "apple", "green")
	// Make the L1 table overlapping the L2 table the selected largest
	// candidate, independent of the filesystem's encoded file sizes.
	database.manifest.SSTables[0].SizeBytes = 100
	database.manifest.SSTables[1].SizeBytes = 10
	database.manifest.NextSSTableID = 3

	flushed := memtable.New()
	flushed.Put(record.Record{
		Key:   "cherry",
		Seq:   10,
		Entry: record.Entry{Value: []byte("red")},
	})
	imm := &memtable.ImmutableMemTable{MemTable: flushed, WalID: 99}
	if err := database.flushImmutableMemtable(imm); err != nil {
		t.Fatalf("flushImmutableMemtable: %v", err)
	}
	database.signalCompaction()
	<-database.compactionDone

	var levels []uint32
	for _, table := range database.sstables {
		levels = append(levels, table.Level)
	}
	wantLevels := []uint32{compaction.L0 + 2, compaction.L0 + 1, compaction.L0}
	if !reflect.DeepEqual(levels, wantLevels) {
		t.Fatalf("SSTable levels after flush = %v, want %v", levels, wantLevels)
	}

	if _, err := os.Stat("data-0.sst"); !os.IsNotExist(err) {
		t.Fatalf("compacted L1 SSTable was not removed: %v", err)
	}
	if _, err := os.Stat("data-1.sst"); err != nil {
		t.Fatalf("unselected L1 SSTable was removed: %v", err)
	}
	if _, err := os.Stat("data-2.sst"); !os.IsNotExist(err) {
		t.Fatalf("overlapping L2 SSTable was not removed: %v", err)
	}

	got, err := database.Get("apple")
	if err != nil {
		t.Fatalf("Get apple: %v", err)
	}
	if string(got) != "green" {
		t.Fatalf("apple = %q, want green", got)
	}
}

func TestFlushRepeatsHigherLevelCompactionUntilBelowThreshold(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 2)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	for id, key := range []string{"apple", "banana", "cherry"} {
		path := fmt.Sprintf("data-%d.sst", id)
		_, err := sstable.CreateFromRecords([]record.Record{{
			Key:   key,
			Seq:   uint64(id + 1),
			Entry: record.Entry{Value: []byte(key)},
		}}, path, sstable.DefaultBlockSize)
		if err != nil {
			t.Fatalf("create SSTable %d: %v", id, err)
		}
		meta := manifest.SSTableMetadata{
			ID:          uint64(id),
			Path:        path,
			Level:       compaction.L0 + 1,
			SmallestKey: key,
			LargestKey:  key,
		}
		database.sstables = append(database.sstables, database.newSSTable(meta))
		database.manifest.SSTables = append(database.manifest.SSTables, meta)
	}
	database.manifest.NextSSTableID = 3

	flushed := memtable.New()
	flushed.Put(record.Record{
		Key:   "date",
		Seq:   10,
		Entry: record.Entry{Value: []byte("date")},
	})
	if err := database.flushImmutableMemtable(&memtable.ImmutableMemTable{
		MemTable: flushed,
		WalID:    99,
	}); err != nil {
		t.Fatalf("flushImmutableMemtable: %v", err)
	}
	database.signalCompaction()
	<-database.compactionDone

	var l1Count, l2Count, l0Count int
	for _, table := range database.manifest.SSTables {
		switch table.Level {
		case compaction.L0:
			l0Count++
		case compaction.L0 + 1:
			l1Count++
		case compaction.L0 + 2:
			l2Count++
		}
	}
	if l1Count != 1 || l2Count != 2 || l0Count != 1 {
		t.Fatalf("level counts = L0:%d L1:%d L2:%d, want L0:1 L1:1 L2:2", l0Count, l1Count, l2Count)
	}

	if _, err := database.Get("apple"); err != nil {
		t.Fatalf("Get apple: %v", err)
	}
	if _, err := database.Get("banana"); err != nil {
		t.Fatalf("Get banana: %v", err)
	}
	if _, err := database.Get("cherry"); err != nil {
		t.Fatalf("Get cherry: %v", err)
	}
	if _, err := database.Get("date"); err != nil {
		t.Fatalf("Get date: %v", err)
	}
}

func TestFlushManifestWriteFailureRollsBackPublishedState(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put apple: %v", err)
	}

	imm := &memtable.ImmutableMemTable{
		MemTable: database.memtable,
		WalID:    database.wal.ID,
	}
	database.immutableMemtables = append(database.immutableMemtables, imm)
	database.manifestPath = filepath.Join("missing-dir", "MANIFEST")

	if err := database.flushImmutableMemtable(imm); err == nil {
		t.Fatal("flushImmutableMemtable succeeded, want manifest write failure")
	}

	if len(database.sstables) != 0 {
		t.Fatalf("sstables = %d, want 0", len(database.sstables))
	}

	if len(database.manifest.SSTables) != 0 {
		t.Fatalf("manifest SSTables = %v, want empty", database.manifest.SSTables)
	}

	if database.manifest.NextSSTableID != 0 {
		t.Fatalf(
			"NextSSTableID = %d, want 0",
			database.manifest.NextSSTableID,
		)
	}

	if _, err := os.Stat("data-0.sst"); !os.IsNotExist(err) {
		t.Fatalf("data-0.sst should be removed after failed flush, got err=%v", err)
	}

	got, err := database.Get("apple")
	if err != nil {
		t.Fatalf("Get apple after failed flush: %v", err)
	}

	if string(got) != "red" {
		t.Fatalf("apple = %q, want red", got)
	}
}

func TestCompactionManifestWriteFailureRollsBackPublishedState(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(100, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer database.Close()

	oldTable, err := sstable.CreateFromRecords([]record.Record{
		{
			Key: "apple",
			Seq: 1,
			Entry: record.Entry{
				Value: []byte("red"),
			},
		},
	}, "data-0.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create old SSTable: %v", err)
	}
	oldTable.ID = 0

	newTable, err := sstable.CreateFromRecords([]record.Record{
		{
			Key: "banana",
			Seq: 2,
			Entry: record.Entry{
				Value: []byte("yellow"),
			},
		},
	}, "data-1.sst", sstable.DefaultBlockSize)
	if err != nil {
		t.Fatalf("create new SSTable: %v", err)
	}
	newTable.ID = 1

	database.sstables = []*sstable.SSTable{oldTable, newTable}
	database.manifest.NextSSTableID = 2
	database.manifest.SSTables = []manifest.SSTableMetadata{
		{
			ID:   oldTable.ID,
			Path: oldTable.Path,
		},
		{
			ID:   newTable.ID,
			Path: newTable.Path,
		},
	}
	database.manifestPath = filepath.Join("missing-dir", "MANIFEST")

	if err := database.Compact(); err == nil {
		t.Fatal("Compact succeeded, want manifest write failure")
	}

	if len(database.sstables) != 2 ||
		database.sstables[0] != oldTable ||
		database.sstables[1] != newTable {
		t.Fatalf("sstables changed after failed compaction: %v", database.sstables)
	}

	if database.manifest.NextSSTableID != 2 {
		t.Fatalf(
			"NextSSTableID = %d, want 2",
			database.manifest.NextSSTableID,
		)
	}

	wantMetadata := []manifest.SSTableMetadata{
		{
			ID:   oldTable.ID,
			Path: oldTable.Path,
		},
		{
			ID:   newTable.ID,
			Path: newTable.Path,
		},
	}
	if !reflect.DeepEqual(database.manifest.SSTables, wantMetadata) {
		t.Fatalf(
			"manifest SSTables = %+v, want %+v",
			database.manifest.SSTables,
			wantMetadata,
		)
	}

	if _, err := os.Stat("compact-2.sst"); !os.IsNotExist(err) {
		t.Fatalf("compact-2.sst should be removed after failed compaction, got err=%v", err)
	}

	got, err := database.Get("apple")
	if err != nil {
		t.Fatalf("Get apple after failed compaction: %v", err)
	}

	if string(got) != "red" {
		t.Fatalf("apple = %q, want red", got)
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(10_000, 100)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer database.Close()

	if err := database.Put("stable", []byte("value")); err != nil {
		t.Fatalf("initial Put: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 250; j++ {
				got, err := database.Get("stable")
				if err != nil {
					t.Errorf("Get stable: %v", err)
					return
				}

				if string(got) != "value" {
					t.Errorf("stable = %q, want value", got)
					return
				}
			}
		}()
	}

	for writer := 0; writer < 4; writer++ {
		wg.Add(1)

		go func(writer int) {
			defer wg.Done()

			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("writer-%d-%d", writer, j)

				if err := database.Put(key, []byte("x")); err != nil {
					t.Errorf("Put %q: %v", key, err)
					return
				}
			}
		}(writer)
	}

	wg.Wait()
}

func TestConcurrentFirstReadsAfterReopen(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(2, 100)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := database.Put("apple", []byte("red")); err != nil {
		t.Fatalf("Put apple: %v", err)
	}
	if err := database.Put("banana", []byte("yellow")); err != nil {
		t.Fatalf("Put banana: %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	reopened, err := New(2, 100)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	var wg sync.WaitGroup

	for i := 0; i < 16; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			got, err := reopened.Get("apple")
			if err != nil {
				t.Errorf("Get apple: %v", err)
				return
			}

			if string(got) != "red" {
				t.Errorf("a = %q, want red", got)
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentGetDuringBackgroundFlush(t *testing.T) {
	useTempWorkingDirectory(t)

	database, err := New(4, 100)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer database.Close()

	if err := database.Put("stable", []byte("value")); err != nil {
		t.Fatalf("Put stable: %v", err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 200; i++ {
			got, err := database.Get("stable")
			if err != nil {
				t.Errorf("Get stable: %v", err)
				return
			}

			if string(got) != "value" {
				t.Errorf("stable = %q, want value", got)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key-%d", i)

			if err := database.Put(key, []byte("x")); err != nil {
				t.Errorf("Put %q: %v", key, err)
				return
			}
		}
	}()

	wg.Wait()
}
