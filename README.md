# HyphaDB

HyphaDB is an embedded key-value database written in Go. Its current focus for the project is building a reliable LSM-tree-style storage engine.

The storage path currently includes:

- An in-memory table backed by a skip list.
- A write-ahead log (WAL) for recovery.
- Sorted string tables (SSTables) with sparse indexes, Bloom filters, compression, and block caching.
- Background memtable flushing and SSTable compaction.
- Point reads, writes, deletes, and ordered range/prefix scans.

The project is under active development and should be treated as experimental. On-disk formats and APIs may change.

## Current API

The database supports the following operations through its Go package:

- `Put(key, value)`
- `Get(key)`
- `Delete(key)`
- `NewIterator` for half-open key ranges (`Start <= key < End`)
- `ScanPrefix` for prefix scans
- `Compact`
- `Close`

Open a database by providing its storage directory. Tuning fields are optional
and use defaults when left at zero:

```go
database, err := hyphadb.Open(hyphadb.Options{DataDir: "./data"})
if err != nil {
	return err
}
defer database.Close()
```

`New` remains available for compatibility but is deprecated in favor of
`Open`, which does not require changing the process working directory and can
represent the complete database configuration.

Deletes are represented as tombstones and are suppressed from reads and scans. Records are ordered by key, with sequence numbers used to resolve newer versions of the same key.

## Recovery and storage

Writes are appended to the WAL before being applied to the active memtable. When the database is opened, WAL segments are replayed before normal operation resumes. Memtables are flushed into SSTables in the background, and compaction merges SSTables as their number grows.

The current implementation keeps `MANIFEST`, WAL segments, SSTables, and its
ownership lock under the configured `DataDir`.

## Development

Run the test suite with:

```sh
go test ./...
go test -race ./...
```

Run the embedded API microbenchmarks with:

```sh
go test -run '^$' -bench . -benchmem ./...
```

The initial benchmarks separate active-memtable reads from persisted-SSTable
warm-cache reads and keep fixture creation and key generation outside the timed
region. No-flush write benchmarks intentionally isolate WAL and memtable costs.
They are intended as repeatable local baselines; cross-database comparisons
will use a shared workload harness with equivalent durability and cache
settings.

Cross-database benchmarks live in the isolated `benchmarks` module. Run the
adapter tests and benchmark smoke test with:

```sh
cd benchmarks
go test ./...
go test -run '^$' -bench . -benchtime=1x ./...
```

See `benchmarks/README.md` for fixed-work comparisons, deterministic dataset
seeds, and physical-filesystem storage configuration.

The module root exposes the embedded Go API. The internal storage engine is in
`internal/db`, and a versioned gRPC storage contract is available under
`proto/hyphadb/v1`.

## Direction

The next architectural step is to establish repeatable benchmark workloads,
then use their results to guide storage-engine optimization before building the
document and embeddings layers.
