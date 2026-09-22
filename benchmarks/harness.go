package benchmarks

import (
	"errors"
	"fmt"
	"log"

	hyphadb "github.com/aaw3/hyphadb"
	"github.com/cockroachdb/pebble/v2"
)

const (
	datasetSize = 10_000
	valueSize   = 128
	batchSize   = 100
	scanSize    = 100
	cacheSize   = 64 * 1024 * 1024
)

type benchmarkKey struct {
	text string
	raw  []byte
}

type benchmarkDB interface {
	Put(benchmarkKey, []byte) error
	Get(benchmarkKey) ([]byte, bool, error)
	NewBatch() benchmarkBatch
	NewIterator(start, end benchmarkKey) (benchmarkIterator, error)
	Close() error
}

type benchmarkBatch interface {
	Put(benchmarkKey, []byte) error
	Commit(sync bool) error
}

type benchmarkIterator interface {
	Prepare() error
	Next() bool
	Bytes() int
	Err() error
	Close() error
}

type engineFactory struct {
	name                   string
	openCompactionDisabled func(string) (benchmarkDB, error)
	openPersisted          func(string, []benchmarkRecord) (benchmarkDB, error)
}

var engines = []engineFactory{
	{
		name:                   "HyphaDB",
		openCompactionDisabled: openHyphaCompactionDisabled,
		openPersisted:          openHyphaPersisted,
	},
	{
		name:                   "Pebble",
		openCompactionDisabled: openPebbleCompactionDisabled,
		openPersisted:          openPebblePersisted,
	},
}

type hyphaDatabase struct {
	db *hyphadb.DB
}

func openHyphaCompactionDisabled(dir string) (benchmarkDB, error) {
	db, err := hyphadb.Open(hyphadb.Options{
		DataDir: dir,
		Memtable: hyphadb.MemtableOptions{
			MaxEntries: maxInt(),
		},
		Compaction: hyphadb.CompactionOptions{
			TableCountThreshold: maxInt(),
		},
		BlockCache: hyphadb.BlockCacheOptions{
			CapacityBytes: cacheSize,
		},
	})
	if err != nil {
		return nil, err
	}
	return &hyphaDatabase{db: db}, nil
}

func openHyphaPersisted(
	dir string,
	records []benchmarkRecord,
) (benchmarkDB, error) {
	options := hyphadb.Options{
		DataDir: dir,
		Memtable: hyphadb.MemtableOptions{
			MaxEntries: len(records),
		},
		Compaction: hyphadb.CompactionOptions{
			TableCountThreshold: maxInt(),
		},
		BlockCache: hyphadb.BlockCacheOptions{
			CapacityBytes: cacheSize,
		},
	}
	db, err := hyphadb.Open(options)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if err := db.Put(record.key.text, record.value); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := db.Close(); err != nil {
		return nil, err
	}

	options.Memtable.MaxEntries = maxInt()
	db, err = hyphadb.Open(options)
	if err != nil {
		return nil, err
	}
	return &hyphaDatabase{db: db}, nil
}

func (db *hyphaDatabase) Put(key benchmarkKey, value []byte) error {
	return db.db.Put(key.text, value)
}

func (db *hyphaDatabase) Get(key benchmarkKey) ([]byte, bool, error) {
	value, err := db.db.Get(key.text)
	if errors.Is(err, hyphadb.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (db *hyphaDatabase) NewBatch() benchmarkBatch {
	return &hyphaBatch{batch: db.db.NewBatch()}
}

func (db *hyphaDatabase) NewIterator(
	start benchmarkKey,
	end benchmarkKey,
) (benchmarkIterator, error) {
	iterator, err := db.db.NewIterator(hyphadb.IteratorOptions{
		Start: start.text,
		End:   end.text,
	})
	if err != nil {
		return nil, err
	}
	return &hyphaIterator{iterator: iterator}, nil
}

func (db *hyphaDatabase) Close() error {
	return db.db.Close()
}

type hyphaBatch struct {
	batch *hyphadb.Batch
}

type hyphaIterator struct {
	iterator *hyphadb.Iterator
	bytes    int
}

func (*hyphaIterator) Prepare() error {
	// HyphaDB eagerly seeks and primes its sources in NewIterator.
	return nil
}

func (iterator *hyphaIterator) Next() bool {
	if !iterator.iterator.Next() {
		return false
	}
	iterator.bytes = len(iterator.iterator.Key()) + len(iterator.iterator.Value())
	return true
}

func (iterator *hyphaIterator) Bytes() int {
	return iterator.bytes
}

func (iterator *hyphaIterator) Err() error {
	return iterator.iterator.Err()
}

func (iterator *hyphaIterator) Close() error {
	return iterator.iterator.Close()
}

func (batch *hyphaBatch) Put(key benchmarkKey, value []byte) error {
	return batch.batch.Put(key.text, value)
}

func (batch *hyphaBatch) Commit(sync bool) error {
	return batch.batch.Commit(hyphadb.WriteOptions{Sync: sync})
}

type pebbleDatabase struct {
	db *pebble.DB
}

type benchmarkPebbleLogger struct{}

func (benchmarkPebbleLogger) Infof(string, ...interface{}) {}

func (benchmarkPebbleLogger) Errorf(format string, args ...interface{}) {
	log.Printf("pebble: "+format, args...)
}

func (benchmarkPebbleLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func pebbleOptions() *pebble.Options {
	return &pebble.Options{
		CacheSize:                   cacheSize,
		MemTableSize:                512 * 1024 * 1024,
		MemTableStopWritesThreshold: 4,
		DisableAutomaticCompactions: true,
		Logger:                      benchmarkPebbleLogger{},
	}
}

func openPebbleCompactionDisabled(dir string) (benchmarkDB, error) {
	db, err := pebble.Open(dir, pebbleOptions())
	if err != nil {
		return nil, err
	}
	return &pebbleDatabase{db: db}, nil
}

func openPebblePersisted(
	dir string,
	records []benchmarkRecord,
) (benchmarkDB, error) {
	db, err := pebble.Open(dir, pebbleOptions())
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if err := db.Set(record.key.raw, record.value, pebble.NoSync); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := db.Flush(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := db.Close(); err != nil {
		return nil, err
	}

	db, err = pebble.Open(dir, pebbleOptions())
	if err != nil {
		return nil, err
	}
	return &pebbleDatabase{db: db}, nil
}

func (db *pebbleDatabase) Put(key benchmarkKey, value []byte) error {
	return db.db.Set(key.raw, value, pebble.NoSync)
}

func (db *pebbleDatabase) Get(key benchmarkKey) ([]byte, bool, error) {
	value, closer, err := db.db.Get(key.raw)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	owned := append([]byte(nil), value...)
	if err := closer.Close(); err != nil {
		return nil, false, err
	}
	return owned, true, nil
}

func (db *pebbleDatabase) NewBatch() benchmarkBatch {
	return &pebbleBatch{batch: db.db.NewBatch()}
}

func (db *pebbleDatabase) NewIterator(
	start benchmarkKey,
	end benchmarkKey,
) (benchmarkIterator, error) {
	iterator, err := db.db.NewIter(&pebble.IterOptions{
		LowerBound: start.raw,
		UpperBound: end.raw,
	})
	if err != nil {
		return nil, err
	}
	return &pebbleIterator{iterator: iterator}, nil
}

func (db *pebbleDatabase) Close() error {
	return db.db.Close()
}

type pebbleBatch struct {
	batch *pebble.Batch
}

type pebbleIterator struct {
	iterator *pebble.Iterator
	prepared bool
	first    bool
	valid    bool
	bytes    int
	err      error
}

func (iterator *pebbleIterator) Prepare() error {
	if iterator.prepared {
		return iterator.err
	}
	iterator.prepared = true
	iterator.first = true
	iterator.valid = iterator.iterator.First()
	if err := iterator.iterator.Error(); err != nil {
		iterator.err = err
	}
	return iterator.err
}

func (iterator *pebbleIterator) Next() bool {
	if iterator.err != nil {
		return false
	}
	if !iterator.prepared {
		if err := iterator.Prepare(); err != nil {
			return false
		}
	}
	if iterator.first {
		iterator.first = false
	} else {
		iterator.valid = iterator.iterator.Next()
	}
	if !iterator.valid {
		return false
	}
	value, err := iterator.iterator.ValueAndErr()
	if err != nil {
		iterator.err = err
		return false
	}
	owned := append([]byte(nil), value...)
	iterator.bytes = len(iterator.iterator.Key()) + len(owned)
	return true
}

func (iterator *pebbleIterator) Bytes() int {
	return iterator.bytes
}

func (iterator *pebbleIterator) Err() error {
	return errors.Join(iterator.err, iterator.iterator.Error())
}

func (iterator *pebbleIterator) Close() error {
	return iterator.iterator.Close()
}

func (batch *pebbleBatch) Put(key benchmarkKey, value []byte) error {
	return batch.batch.Set(key.raw, value, nil)
}

func (batch *pebbleBatch) Commit(sync bool) error {
	writeOptions := pebble.NoSync
	if sync {
		writeOptions = pebble.Sync
	}
	commitErr := batch.batch.Commit(writeOptions)
	closeErr := batch.batch.Close()
	return errors.Join(commitErr, closeErr)
}

func missingKeys(keys []benchmarkKey) []benchmarkKey {
	missing := make([]benchmarkKey, len(keys)-1)
	for i, key := range keys[:len(keys)-1] {
		text := key.text + "/missing"
		missing[i] = benchmarkKey{text: text, raw: []byte(text)}
	}
	return missing
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
