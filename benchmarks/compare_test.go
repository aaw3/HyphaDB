package benchmarks

import (
	"bytes"
	"encoding/hex"
	"flag"
	"os"
	"runtime"
	"testing"
)

var benchmarkStorageRoot = flag.String(
	"storage-root",
	"",
	"directory under which benchmark database directories are created",
)

var (
	resultBytes []byte
	resultInt   int
)

func TestEngineAdapters(t *testing.T) {
	records := makeDataset(4, 16, defaultBenchmarkSeed)
	keys := datasetKeys(records)

	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			database, err := engine.openCompactionDisabled(t.TempDir())
			if err != nil {
				t.Fatalf("open: %v", err)
			}

			if err := database.Put(records[0].key, records[0].value); err != nil {
				t.Fatalf("Put: %v", err)
			}
			got, found, err := database.Get(keys[0])
			if err != nil || !found || !bytes.Equal(got, records[0].value) {
				t.Fatalf("Get = %d bytes, found=%t, err=%v", len(got), found, err)
			}

			batch := database.NewBatch()
			for _, record := range records[1:] {
				if err := batch.Put(record.key, record.value); err != nil {
					t.Fatalf("batch Put: %v", err)
				}
			}
			if err := batch.Commit(false); err != nil {
				t.Fatalf("batch Commit: %v", err)
			}

			scanned, _, err := database.Scan(keys[0], benchmarkKey{
				text: "key/99999999999999999999",
				raw:  []byte("key/99999999999999999999"),
			})
			if err != nil || scanned != len(keys) {
				t.Fatalf("Scan = %d records, err=%v", scanned, err)
			}
			if err := database.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			persisted, err := engine.openPersisted(t.TempDir(), records)
			if err != nil {
				t.Fatalf("open persisted fixture: %v", err)
			}
			got, found, err = persisted.Get(keys[2])
			if err != nil || !found || !bytes.Equal(got, records[2].value) {
				t.Fatalf("persisted Get found=%t, err=%v", found, err)
			}
			if err := persisted.Close(); err != nil {
				t.Fatalf("close persisted fixture: %v", err)
			}
		})
	}
}

func TestDatasetIsReproducible(t *testing.T) {
	first := makeDataset(4, 16, defaultBenchmarkSeed)
	second := makeDataset(4, 16, defaultBenchmarkSeed)
	changed := makeDataset(4, 16, defaultBenchmarkSeed+1)

	for index := range first {
		if first[index].key.text != second[index].key.text ||
			!bytes.Equal(first[index].value, second[index].value) {
			t.Fatalf("record %d differs for the same seed", index)
		}
	}
	if bytes.Equal(first[0].value, changed[0].value) {
		t.Fatal("different seeds generated the same first value")
	}
	if got, want := hex.EncodeToString(first[0].value),
		"4fd0f0813e6a9097f5d01c01c72f32b0"; got != want {
		t.Fatalf(
			"dataset v%d first value = %s, want %s; increment the dataset version if intentional",
			benchmarkDatasetVersion,
			got,
			want,
		)
	}

	generator := splitMix64{}
	if got, want := generator.next(), uint64(0xe220a8397b1dcdaf); got != want {
		t.Fatalf("first SplitMix64 value = %#x, want %#x", got, want)
	}
}

func BenchmarkComparison(b *testing.B) {
	b.Run("PutSequentialCompactionDisabled", benchmarkPutSequentialCompactionDisabled)
	b.Run("BatchPutUnique100CompactionDisabled", benchmarkBatchPutUniqueCompactionDisabled)
	b.Run("BatchOverwrite100CompactionDisabled", benchmarkBatchOverwriteCompactionDisabled)
	b.Run("GetMemtable", benchmarkGetMemtable)
	b.Run("GetPersistedWarm", benchmarkGetPersistedWarm)
	b.Run("ScanPersistedWarm100", benchmarkScanPersistedWarm)
}

func benchmarkPutSequentialCompactionDisabled(b *testing.B) {
	for _, engine := range engines {
		b.Run(engine.name, func(b *testing.B) {
			records := makeDataset(b.N, valueSize, *benchmarkSeed)
			database := openCompactionDisabledBenchmarkDB(b, engine)

			b.ReportAllocs()
			b.SetBytes(valueSize)
			startMeasuredRun(b)
			for _, record := range records {
				if err := database.Put(record.key, record.value); err != nil {
					b.Fatalf("Put: %v", err)
				}
			}
			reportRecords(b, 1)
		})
	}
}

func benchmarkBatchPutUniqueCompactionDisabled(b *testing.B) {
	for _, syncWrites := range []bool{false, true} {
		b.Run(syncName(syncWrites), func(b *testing.B) {
			for _, engine := range engines {
				b.Run(engine.name, func(b *testing.B) {
					records := makeDataset(b.N*batchSize, valueSize, *benchmarkSeed)
					database := openCompactionDisabledBenchmarkDB(b, engine)

					b.ReportAllocs()
					b.SetBytes(valueSize * batchSize)
					startMeasuredRun(b)
					for i := 0; i < b.N; i++ {
						batch := database.NewBatch()
						start := i * batchSize
						for _, record := range records[start : start+batchSize] {
							if err := batch.Put(record.key, record.value); err != nil {
								b.Fatalf("batch Put: %v", err)
							}
						}
						if err := batch.Commit(syncWrites); err != nil {
							b.Fatalf("batch Commit: %v", err)
						}
					}
					reportRecords(b, batchSize)
				})
			}
		})
	}
}

func benchmarkBatchOverwriteCompactionDisabled(b *testing.B) {
	records := makeDataset(batchSize, valueSize, *benchmarkSeed)
	for _, syncWrites := range []bool{false, true} {
		b.Run(syncName(syncWrites), func(b *testing.B) {
			for _, engine := range engines {
				b.Run(engine.name, func(b *testing.B) {
					database := openCompactionDisabledBenchmarkDB(b, engine)

					b.ReportAllocs()
					b.SetBytes(int64(valueSize * len(records)))
					startMeasuredRun(b)
					for i := 0; i < b.N; i++ {
						batch := database.NewBatch()
						for _, record := range records {
							if err := batch.Put(record.key, record.value); err != nil {
								b.Fatalf("batch Put: %v", err)
							}
						}
						if err := batch.Commit(syncWrites); err != nil {
							b.Fatalf("batch Commit: %v", err)
						}
					}
					reportRecords(b, len(records))
				})
			}
		})
	}
}

func benchmarkGetMemtable(b *testing.B) {
	records := makeDataset(datasetSize, valueSize, *benchmarkSeed)
	keys := datasetKeys(records)
	readKeys := shuffledKeys(keys, *benchmarkSeed)
	missing := benchmarkKey{text: "missing/key", raw: []byte("missing/key")}

	for _, engine := range engines {
		b.Run(engine.name, func(b *testing.B) {
			database := openCompactionDisabledBenchmarkDB(b, engine)
			loadRecords(b, database, records)

			b.Run("Hit", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(valueSize)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, found, err := database.Get(readKeys[i%len(readKeys)])
					if err != nil || !found {
						b.Fatalf("Get found=%t, err=%v", found, err)
					}
					resultBytes = got
				}
			})

			b.Run("Miss", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, found, err := database.Get(missing); err != nil || found {
						b.Fatalf("Get missing found=%t, err=%v", found, err)
					}
				}
			})

			b.Run("ParallelHit", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(valueSize)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					index := 0
					var got []byte
					for pb.Next() {
						var found bool
						var err error
						got, found, err = database.Get(readKeys[index%len(readKeys)])
						if err != nil || !found {
							b.Errorf("Get found=%t, err=%v", found, err)
							return
						}
						index++
					}
					runtime.KeepAlive(got)
				})
			})
		})
	}
}

func benchmarkGetPersistedWarm(b *testing.B) {
	records := makeDataset(datasetSize, valueSize, *benchmarkSeed)
	keys := datasetKeys(records)
	readKeys := shuffledKeys(keys, *benchmarkSeed)
	misses := shuffledKeys(missingKeys(keys), *benchmarkSeed)

	for _, engine := range engines {
		b.Run(engine.name, func(b *testing.B) {
			database := openPersistedBenchmarkDB(b, engine, records)
			warmReads(b, database, readKeys)

			b.Run("Hit", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(valueSize)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					got, found, err := database.Get(readKeys[i%len(readKeys)])
					if err != nil || !found {
						b.Fatalf("Get found=%t, err=%v", found, err)
					}
					resultBytes = got
				}
			})

			b.Run("Miss", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, found, err := database.Get(misses[i%len(misses)]); err != nil || found {
						b.Fatalf("Get missing found=%t, err=%v", found, err)
					}
				}
			})
		})
	}
}

func benchmarkScanPersistedWarm(b *testing.B) {
	records := makeDataset(datasetSize, valueSize, *benchmarkSeed)
	keys := datasetKeys(records)
	maxStart := len(keys) - scanSize

	for _, engine := range engines {
		b.Run(engine.name, func(b *testing.B) {
			database := openPersistedBenchmarkDB(b, engine, records)
			warmReads(b, database, shuffledKeys(keys, *benchmarkSeed))

			b.ReportAllocs()
			b.SetBytes(scanSize * valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := (i * 97) % maxStart
				records, bytesRead, err := database.Scan(keys[start], keys[start+scanSize])
				if err != nil {
					b.Fatalf("Scan: %v", err)
				}
				if records != scanSize {
					b.Fatalf("Scan returned %d records, want %d", records, scanSize)
				}
				resultInt = bytesRead
			}
			reportRecords(b, scanSize)
		})
	}
}

func openCompactionDisabledBenchmarkDB(b *testing.B, engine engineFactory) benchmarkDB {
	b.Helper()
	b.StopTimer()
	database, err := engine.openCompactionDisabled(benchmarkDataDir(b, engine.name))
	if err != nil {
		b.Fatalf("open %s: %v", engine.name, err)
	}
	b.Cleanup(func() {
		if err := database.Close(); err != nil {
			b.Errorf("close %s: %v", engine.name, err)
		}
	})
	b.StartTimer()
	return database
}

func openPersistedBenchmarkDB(
	b *testing.B,
	engine engineFactory,
	records []benchmarkRecord,
) benchmarkDB {
	b.Helper()
	b.StopTimer()
	database, err := engine.openPersisted(benchmarkDataDir(b, engine.name), records)
	if err != nil {
		b.Fatalf("open persisted %s fixture: %v", engine.name, err)
	}
	b.Cleanup(func() {
		if err := database.Close(); err != nil {
			b.Errorf("close %s: %v", engine.name, err)
		}
	})
	b.StartTimer()
	return database
}

func loadRecords(
	b *testing.B,
	database benchmarkDB,
	records []benchmarkRecord,
) {
	b.Helper()
	b.StopTimer()
	for _, record := range records {
		if err := database.Put(record.key, record.value); err != nil {
			b.Fatalf("load fixture: %v", err)
		}
	}
	b.StartTimer()
}

func warmReads(b *testing.B, database benchmarkDB, keys []benchmarkKey) {
	b.Helper()
	b.StopTimer()
	for _, key := range keys {
		if _, found, err := database.Get(key); err != nil || !found {
			b.Fatalf("warm read found=%t, err=%v", found, err)
		}
	}
	runtime.GC()
	b.StartTimer()
}

func reportRecords(b *testing.B, recordsPerOperation int) {
	b.Helper()
	b.StopTimer()
	b.ReportMetric(
		float64(b.N*recordsPerOperation)/b.Elapsed().Seconds(),
		"records/s",
	)
	b.ReportMetric(float64(recordsPerOperation), "records/op")
	b.ReportMetric(float64(b.N)*float64(recordsPerOperation), "total-records")
}

func startMeasuredRun(b *testing.B) {
	b.Helper()
	b.StopTimer()
	runtime.GC()
	b.ResetTimer()
	b.StartTimer()
}

func benchmarkDataDir(b *testing.B, engine string) string {
	b.Helper()
	if *benchmarkStorageRoot == "" {
		return b.TempDir()
	}
	if err := os.MkdirAll(*benchmarkStorageRoot, 0o700); err != nil {
		b.Fatalf("create benchmark storage root: %v", err)
	}
	dir, err := os.MkdirTemp(*benchmarkStorageRoot, engine+"-")
	if err != nil {
		b.Fatalf("create benchmark data directory: %v", err)
	}
	b.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Errorf("remove benchmark data directory: %v", err)
		}
	})
	return dir
}

func syncName(sync bool) string {
	if sync {
		return "Sync"
	}
	return "Async"
}
