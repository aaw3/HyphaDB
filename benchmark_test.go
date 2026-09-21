package hyphadb_test

import (
	"errors"
	"fmt"
	"runtime"
	"testing"

	hyphadb "github.com/aaw3/hyphadb"
)

const (
	benchmarkDatasetSize = 10_000
	benchmarkValueSize   = 128
	benchmarkScanSize    = 100
	benchmarkBatchSize   = 100
)

var (
	benchmarkBytes  []byte
	benchmarkKey    string
	benchmarkRecord int
)

func BenchmarkPutSequentialNoFlush(b *testing.B) {
	keys := benchmarkKeys(b.N)
	value := benchmarkValue(benchmarkValueSize)
	database := openBenchmarkDB(b, noFlushBenchmarkOptions(b.TempDir()))

	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := database.Put(keys[i], value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

func BenchmarkBatchPutUnique100NoFlush(b *testing.B) {
	value := benchmarkValue(benchmarkValueSize)

	for _, syncWrites := range []bool{false, true} {
		name := "Async"
		if syncWrites {
			name = "Sync"
		}
		b.Run(name, func(b *testing.B) {
			keys := benchmarkKeys(b.N * benchmarkBatchSize)
			database := openBenchmarkDB(b, noFlushBenchmarkOptions(b.TempDir()))

			b.ReportAllocs()
			b.SetBytes(int64(len(value) * benchmarkBatchSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := database.NewBatch()
				start := i * benchmarkBatchSize
				for _, key := range keys[start : start+benchmarkBatchSize] {
					if err := batch.Put(key, value); err != nil {
						b.Fatalf("batch Put: %v", err)
					}
				}
				if err := batch.Commit(hyphadb.WriteOptions{Sync: syncWrites}); err != nil {
					b.Fatalf("batch Commit: %v", err)
				}
			}
			reportRecordThroughput(b, benchmarkBatchSize)
		})
	}
}

func BenchmarkBatchOverwrite100NoFlush(b *testing.B) {
	keys := benchmarkKeys(benchmarkBatchSize)
	value := benchmarkValue(benchmarkValueSize)

	for _, syncWrites := range []bool{false, true} {
		name := "Async"
		if syncWrites {
			name = "Sync"
		}
		b.Run(name, func(b *testing.B) {
			database := openBenchmarkDB(b, noFlushBenchmarkOptions(b.TempDir()))

			b.ReportAllocs()
			b.SetBytes(int64(len(value) * len(keys)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := database.NewBatch()
				for _, key := range keys {
					if err := batch.Put(key, value); err != nil {
						b.Fatalf("batch Put: %v", err)
					}
				}
				if err := batch.Commit(hyphadb.WriteOptions{Sync: syncWrites}); err != nil {
					b.Fatalf("batch Commit: %v", err)
				}
			}
			reportRecordThroughput(b, len(keys))
		})
	}
}

func BenchmarkGetMemtable(b *testing.B) {
	keys := benchmarkKeys(benchmarkDatasetSize)
	value := benchmarkValue(benchmarkValueSize)
	database := openBenchmarkDB(b, noFlushBenchmarkOptions(b.TempDir()))
	loadBenchmarkRecords(b, database, keys, value)
	readKeys := benchmarkShuffledKeys(keys)

	b.Run("Hit", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, err := database.Get(readKeys[i%len(readKeys)])
			if err != nil {
				b.Fatalf("Get: %v", err)
			}
			benchmarkBytes = got
		}
	})

	b.Run("Miss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := database.Get("missing/key"); !errors.Is(err, hyphadb.ErrNotFound) {
				b.Fatalf("Get missing key: %v", err)
			}
		}
	})

	b.Run("ParallelHit", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			index := 0
			var got []byte
			for pb.Next() {
				var err error
				got, err = database.Get(readKeys[index%len(readKeys)])
				if err != nil {
					b.Errorf("Get: %v", err)
					return
				}
				index++
			}
			runtime.KeepAlive(got)
		})
	})
}

func BenchmarkGetSSTableWarm(b *testing.B) {
	database, keys, value := openSSTableBenchmarkDB(b)
	readKeys := benchmarkShuffledKeys(keys)
	missingKeys := benchmarkShuffledKeys(benchmarkMissingKeys(keys))
	warmBenchmarkReads(b, database, readKeys)

	b.Run("Hit", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(value)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			got, err := database.Get(readKeys[i%len(readKeys)])
			if err != nil {
				b.Fatalf("Get: %v", err)
			}
			benchmarkBytes = got
		}
	})

	b.Run("BloomFilteredMiss", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := missingKeys[i%len(missingKeys)]
			if _, err := database.Get(key); !errors.Is(err, hyphadb.ErrNotFound) {
				b.Fatalf("Get missing key: %v", err)
			}
		}
	})
}

func BenchmarkScanSSTableWarm100(b *testing.B) {
	database, keys, value := openSSTableBenchmarkDB(b)
	maxStart := len(keys) - benchmarkScanSize
	warmBenchmarkReads(b, database, benchmarkShuffledKeys(keys))

	b.ReportAllocs()
	b.SetBytes(int64(benchmarkScanSize * len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := (i * 97) % maxStart
		iterator, err := database.NewIterator(hyphadb.IteratorOptions{
			Start: keys[start],
			End:   keys[start+benchmarkScanSize],
		})
		if err != nil {
			b.Fatalf("NewIterator: %v", err)
		}

		count := 0
		for iterator.Next() {
			benchmarkKey = iterator.Key()
			benchmarkBytes = iterator.Value()
			count++
		}
		if err := iterator.Err(); err != nil {
			b.Fatalf("iterator: %v", err)
		}
		if err := iterator.Close(); err != nil {
			b.Fatalf("close iterator: %v", err)
		}
		if count != benchmarkScanSize {
			b.Fatalf("scan returned %d records, want %d", count, benchmarkScanSize)
		}
		benchmarkRecord = count
	}
	b.StopTimer()
	b.ReportMetric(
		float64(b.N*benchmarkScanSize)/b.Elapsed().Seconds(),
		"records/s",
	)
}

func openSSTableBenchmarkDB(b *testing.B) (*hyphadb.DB, []string, []byte) {
	b.Helper()

	dataDir := b.TempDir()
	keys := benchmarkKeys(benchmarkDatasetSize)
	value := benchmarkValue(benchmarkValueSize)
	options := hyphadb.Options{
		DataDir: dataDir,
		Memtable: hyphadb.MemtableOptions{
			MaxEntries: benchmarkDatasetSize,
		},
		Compaction: hyphadb.CompactionOptions{
			TableCountThreshold: maxInt(),
		},
	}

	database := openBenchmarkDB(b, options)
	loadBenchmarkRecords(b, database, keys, value)
	if err := database.Close(); err != nil {
		b.Fatalf("close database after fixture load: %v", err)
	}

	options.Memtable.MaxEntries = maxInt()
	database = openBenchmarkDB(b, options)
	return database, keys, value
}

func openBenchmarkDB(b *testing.B, options hyphadb.Options) *hyphadb.DB {
	b.Helper()

	database, err := hyphadb.Open(options)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() {
		if err := database.Close(); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	return database
}

func loadBenchmarkRecords(b *testing.B, database *hyphadb.DB, keys []string, value []byte) {
	b.Helper()
	b.StopTimer()
	for _, key := range keys {
		if err := database.Put(key, value); err != nil {
			b.Fatalf("load fixture: %v", err)
		}
	}
	b.StartTimer()
}

func benchmarkKeys(count int) []string {
	keys := make([]string, count)
	for i := range keys {
		keys[i] = fmt.Sprintf("key/%020d", i)
	}
	return keys
}

func benchmarkValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(i)
	}
	return value
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func noFlushBenchmarkOptions(dataDir string) hyphadb.Options {
	return hyphadb.Options{
		DataDir: dataDir,
		Memtable: hyphadb.MemtableOptions{
			MaxEntries: maxInt(),
		},
		Compaction: hyphadb.CompactionOptions{
			TableCountThreshold: maxInt(),
		},
	}
}

func benchmarkShuffledKeys(keys []string) []string {
	shuffled := append([]string(nil), keys...)
	state := uint64(0x9e3779b97f4a7c15)
	for i := len(shuffled) - 1; i > 0; i-- {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		j := int(state % uint64(i+1))
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	return shuffled
}

func benchmarkMissingKeys(keys []string) []string {
	// Skip the largest key so every miss remains inside the SSTable's key
	// bounds and reaches the Bloom-filter lookup path.
	missing := make([]string, len(keys)-1)
	for i, key := range keys[:len(keys)-1] {
		missing[i] = key + "/missing"
	}
	return missing
}

func warmBenchmarkReads(b *testing.B, database *hyphadb.DB, keys []string) {
	b.Helper()
	b.StopTimer()
	for _, key := range keys {
		if _, err := database.Get(key); err != nil {
			b.Fatalf("warm block cache: %v", err)
		}
	}
	runtime.GC()
	b.StartTimer()
}

func reportRecordThroughput(b *testing.B, recordsPerOperation int) {
	b.Helper()
	b.StopTimer()
	b.ReportMetric(
		float64(b.N*recordsPerOperation)/b.Elapsed().Seconds(),
		"records/s",
	)
}
