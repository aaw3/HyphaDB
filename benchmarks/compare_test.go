package benchmarks

import (
	"bytes"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
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

			scanned, _, err := scanRange(database, keys[0], benchmarkKey{
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

			multi, err := engine.openPersistedTables(
				t.TempDir(),
				makeDataset(8, 16, defaultBenchmarkSeed),
				4,
			)
			if err != nil {
				t.Fatalf("open multi-table fixture: %v", err)
			}
			if _, found, err := multi.Get(benchmarkKey{
				text: "key/00000000000000000006",
				raw:  []byte("key/00000000000000000006"),
			}); err != nil || !found {
				t.Fatalf("multi-table Get found=%t, err=%v", found, err)
			}
			if err := multi.Close(); err != nil {
				t.Fatalf("close multi-table fixture: %v", err)
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
	b.Run("GetPersistedWarmMultiTable", benchmarkGetPersistedWarmMultiTable)
	b.Run("GetPersistedWarmValueSizes", benchmarkGetPersistedWarmValueSizes)
	b.Run("IteratorCreatePersistedWarm", benchmarkIteratorCreatePersistedWarm)
	b.Run("IteratorTraversePersistedWarm100", benchmarkIteratorTraversePersistedWarm)
	b.Run("ScanPersistedWarm100", benchmarkScanPersistedWarm)
	b.Run("ScanPersistedWarmSizes", benchmarkScanPersistedWarmSizes)
	b.Run("ScanPersistedWarmMultiTable100", benchmarkScanPersistedWarmMultiTable)
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
	outOfRangeKeys := benchmarkKeys(
		"zzzz/out-of-range/0001",
		"zzzz/out-of-range/0002",
		"zzzz/out-of-range/0003",
		"zzzz/out-of-range/0004",
	)

	for _, bloomCase := range []struct {
		name    string
		enabled bool
	}{
		{name: "BloomEnabled", enabled: true},
		{name: "BloomDisabled", enabled: false},
	} {
		b.Run(bloomCase.name, func(b *testing.B) {
			for _, engine := range engines {
				b.Run(engine.name, func(b *testing.B) {
					database := openPersistedBloomBenchmarkDB(
						b,
						engine,
						records,
						bloomCase.enabled,
					)
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

					b.Run("InRangeMiss", func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							key := misses[i%len(misses)]
							if _, found, err := database.Get(key); err != nil || found {
								b.Fatalf("Get missing found=%t, err=%v", found, err)
							}
						}
					})

					b.Run("OutOfRangeMiss", func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							key := outOfRangeKeys[i%len(outOfRangeKeys)]
							if _, found, err := database.Get(key); err != nil || found {
								b.Fatalf("Get out-of-range found=%t, err=%v", found, err)
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
									b.Errorf("parallel Get found=%t, err=%v", found, err)
									return
								}
								index++
							}
							runtime.KeepAlive(got)
						})
					})
				})
			}
		})
	}
}

func benchmarkGetPersistedWarmMultiTable(b *testing.B) {
	records := makeDataset(datasetSize, valueSize, *benchmarkSeed)
	keys := datasetKeys(records)
	recordsPerTable := len(records) / multiTableCount
	lowKeys := shuffledKeys(keys[:recordsPerTable], *benchmarkSeed)
	highKeys := shuffledKeys(keys[len(keys)-recordsPerTable:], *benchmarkSeed)
	misses := shuffledKeys(missingKeys(keys), *benchmarkSeed)
	outOfRangeKeys := benchmarkKeys(
		"zzzz/out-of-range/0001",
		"zzzz/out-of-range/0002",
		"zzzz/out-of-range/0003",
		"zzzz/out-of-range/0004",
	)

	for _, engine := range engines {
		b.Run(engine.name, func(b *testing.B) {
			database := openPersistedTablesBenchmarkDB(
				b,
				engine,
				records,
				multiTableCount,
			)
			warmReads(b, database, shuffledKeys(keys, *benchmarkSeed))

			for _, readCase := range []struct {
				name string
				keys []benchmarkKey
			}{
				{name: "LowRangeHit", keys: lowKeys},
				{name: "HighRangeHit", keys: highKeys},
			} {
				b.Run(readCase.name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(valueSize)
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						got, found, err := database.Get(readCase.keys[i%len(readCase.keys)])
						if err != nil || !found {
							b.Fatalf("Get found=%t, err=%v", found, err)
						}
						resultBytes = got
					}
					reportRecords(b, 1)
				})
			}

			b.Run("InRangeBloomMiss", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, found, err := database.Get(misses[i%len(misses)]); err != nil || found {
						b.Fatalf("Get missing found=%t, err=%v", found, err)
					}
				}
				reportRecords(b, 1)
			})

			b.Run("OutOfRangeMiss", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					key := outOfRangeKeys[i%len(outOfRangeKeys)]
					if _, found, err := database.Get(key); err != nil || found {
						b.Fatalf("Get out-of-range found=%t, err=%v", found, err)
					}
				}
				reportRecords(b, 1)
			})

			b.Run("ParallelHit", func(b *testing.B) {
				readKeys := shuffledKeys(keys, *benchmarkSeed)
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
							b.Errorf("parallel Get found=%t, err=%v", found, err)
							return
						}
						index++
					}
					runtime.KeepAlive(got)
				})
				reportRecords(b, 1)
			})
		})
	}
}

func benchmarkKeys(keys ...string) []benchmarkKey {
	result := make([]benchmarkKey, len(keys))
	for i, key := range keys {
		result[i] = benchmarkKey{text: key, raw: []byte(key)}
	}
	return result
}

func benchmarkGetPersistedWarmValueSizes(b *testing.B) {
	for _, size := range extendedValueSizes {
		b.Run(fmt.Sprintf("Value%dB", size), func(b *testing.B) {
			records := makeDataset(valueMatrixSize, size, *benchmarkSeed)
			keys := shuffledKeys(datasetKeys(records), *benchmarkSeed)

			for _, engine := range engines {
				b.Run(engine.name, func(b *testing.B) {
					database := openPersistedBenchmarkDB(b, engine, records)
					warmReads(b, database, keys)

					b.ReportAllocs()
					b.SetBytes(int64(size))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						got, found, err := database.Get(keys[i%len(keys)])
						if err != nil || !found {
							b.Fatalf("Get found=%t, err=%v", found, err)
						}
						resultBytes = got
					}
					reportRecords(b, 1)
				})
			}
		})
	}
}

func benchmarkScanPersistedWarm(b *testing.B) {
	records := makeDataset(datasetSize, valueSize, *benchmarkSeed)
	keys := datasetKeys(records)

	for _, engine := range engines {
		b.Run(engine.name, func(b *testing.B) {
			database := openPersistedBenchmarkDB(b, engine, records)
			warmReads(b, database, shuffledKeys(keys, *benchmarkSeed))

			b.ReportAllocs()
			b.SetBytes(scanSize * valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start, end := scanBounds(keys, i)
				records, bytesRead, err := scanRange(database, start, end)
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

func benchmarkScanPersistedWarmSizes(b *testing.B) {
	records := makeDataset(datasetSize, valueSize, *benchmarkSeed)
	keys := datasetKeys(records)

	for _, size := range extendedScanSizes {
		b.Run(fmt.Sprintf("Records%d", size), func(b *testing.B) {
			for _, engine := range engines {
				b.Run(engine.name, func(b *testing.B) {
					database := openPersistedBenchmarkDB(b, engine, records)
					warmReads(b, database, shuffledKeys(keys, *benchmarkSeed))

					b.ReportAllocs()
					b.SetBytes(int64(size * valueSize))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						start, end := scanBoundsForSize(keys, i, size)
						count, bytesRead, err := scanRange(database, start, end)
						if err != nil {
							b.Fatalf("Scan: %v", err)
						}
						if count != size {
							b.Fatalf("Scan returned %d records, want %d", count, size)
						}
						resultInt = bytesRead
					}
					reportRecords(b, size)
				})
			}
		})
	}
}

func benchmarkScanPersistedWarmMultiTable(b *testing.B) {
	records := makeDataset(datasetSize, valueSize, *benchmarkSeed)
	keys := datasetKeys(records)

	for _, engine := range engines {
		b.Run(engine.name, func(b *testing.B) {
			database := openPersistedTablesBenchmarkDB(
				b,
				engine,
				records,
				multiTableCount,
			)
			warmReads(b, database, shuffledKeys(keys, *benchmarkSeed))

			b.ReportAllocs()
			b.SetBytes(scanSize * valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start, end := scanBoundsForSize(keys, i, scanSize)
				count, bytesRead, err := scanRange(database, start, end)
				if err != nil {
					b.Fatalf("Scan: %v", err)
				}
				if count != scanSize {
					b.Fatalf("Scan returned %d records, want %d", count, scanSize)
				}
				resultInt = bytesRead
			}
			reportRecords(b, scanSize)
		})
	}
}

func benchmarkIteratorCreatePersistedWarm(b *testing.B) {
	records := makeDataset(datasetSize, valueSize, *benchmarkSeed)
	keys := datasetKeys(records)

	for _, engine := range engines {
		b.Run(engine.name, func(b *testing.B) {
			database := openPersistedBenchmarkDB(b, engine, records)
			warmReads(b, database, shuffledKeys(keys, *benchmarkSeed))

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start, end := scanBounds(keys, i)
				iterator, err := database.NewIterator(start, end)
				if err != nil {
					b.Fatalf("NewIterator: %v", err)
				}
				if err := iterator.Prepare(); err != nil {
					_ = iterator.Close()
					b.Fatalf("prepare iterator: %v", err)
				}
				b.StopTimer()
				if err := iterator.Close(); err != nil {
					b.Fatalf("close iterator: %v", err)
				}
				b.StartTimer()
			}
		})
	}
}

func benchmarkIteratorTraversePersistedWarm(b *testing.B) {
	records := makeDataset(datasetSize, valueSize, *benchmarkSeed)
	keys := datasetKeys(records)

	for _, engine := range engines {
		b.Run(engine.name, func(b *testing.B) {
			database := openPersistedBenchmarkDB(b, engine, records)
			warmReads(b, database, shuffledKeys(keys, *benchmarkSeed))

			b.ReportAllocs()
			b.SetBytes(scanSize * valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				start, end := scanBounds(keys, i)
				iterator, err := database.NewIterator(start, end)
				if err == nil {
					err = iterator.Prepare()
				}
				if err != nil {
					if iterator != nil {
						_ = iterator.Close()
					}
					b.Fatalf("prepare iterator: %v", err)
				}
				b.StartTimer()

				count, bytesRead := drainIterator(iterator)

				b.StopTimer()
				iteratorErr := iterator.Err()
				closeErr := iterator.Close()
				if err := errors.Join(iteratorErr, closeErr); err != nil {
					b.Fatalf("iterate: %v", err)
				}
				if count != scanSize {
					b.Fatalf("iterator returned %d records, want %d", count, scanSize)
				}
				resultInt = bytesRead
				b.StartTimer()
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

func openPersistedBloomBenchmarkDB(
	b *testing.B,
	engine engineFactory,
	records []benchmarkRecord,
	bloomEnabled bool,
) benchmarkDB {
	b.Helper()
	b.StopTimer()
	database, err := engine.openPersistedWithBloom(
		benchmarkDataDir(b, engine.name),
		records,
		bloomEnabled,
	)
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

func openPersistedTablesBenchmarkDB(
	b *testing.B,
	engine engineFactory,
	records []benchmarkRecord,
	tableCount int,
) benchmarkDB {
	b.Helper()
	b.StopTimer()
	database, err := engine.openPersistedTables(
		benchmarkDataDir(b, engine.name),
		records,
		tableCount,
	)
	if err != nil {
		b.Fatalf("open persisted %s multi-table fixture: %v", engine.name, err)
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

func scanBounds(keys []benchmarkKey, operation int) (benchmarkKey, benchmarkKey) {
	return scanBoundsForSize(keys, operation, scanSize)
}

func scanBoundsForSize(
	keys []benchmarkKey,
	operation int,
	size int,
) (benchmarkKey, benchmarkKey) {
	maxStart := len(keys) - size
	start := (operation * 97) % maxStart
	return keys[start], keys[start+size]
}

func scanRange(
	database benchmarkDB,
	start benchmarkKey,
	end benchmarkKey,
) (int, int, error) {
	iterator, err := database.NewIterator(start, end)
	if err != nil {
		return 0, 0, err
	}
	if err := iterator.Prepare(); err != nil {
		return 0, 0, errors.Join(err, iterator.Close())
	}
	records, bytesRead := drainIterator(iterator)
	return records, bytesRead, errors.Join(iterator.Err(), iterator.Close())
}

func drainIterator(iterator benchmarkIterator) (int, int) {
	records := 0
	bytesRead := 0
	for iterator.Next() {
		bytesRead += iterator.Bytes()
		records++
	}
	return records, bytesRead
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
