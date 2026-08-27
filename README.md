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

Deletes are represented as tombstones and are suppressed from reads and scans. Records are ordered by key, with sequence numbers used to resolve newer versions of the same key.

## Recovery and storage

Writes are appended to the WAL before being applied to the active memtable. When the database is opened, WAL segments are replayed before normal operation resumes. Memtables are flushed into SSTables in the background, and compaction merges SSTables as their number grows.

The current implementation uses files in the process working directory, including `MANIFEST`, WAL segments, and SSTables.

## Development

Run the test suite with:

```sh
go test ./...
go test -race ./...
```

The reusable database implementation is currently in `internal/db`. A public package, command-line interface, and server interface are currently in the works.

## Direction

The next architectural step is to stabilize the storage API and configuration, then place a service layer in front of it. The likely network interface is versioned gRPC over TCP, with gRPC over a Unix domain socket for local clients. This will allow clients in other languages to use generated, typed APIs without depending on HyphaDB's on-disk formats or internal packages.
