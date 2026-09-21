# Storage-engine comparison benchmarks

This nested Go module compares HyphaDB with benchmark-only database
dependencies without adding those dependencies to the main storage module.
Pebble v2 is added as the first comparison adapter.

Run the adapter tests and a short benchmark smoke test:

```sh
go test ./...
go test -run '^$' -bench . -benchtime=1x ./...
```

The smoke test verifies that workloads execute; its measurements are not a
performance comparison.

## Controlled comparisons

State-changing benchmarks must use fixed iteration counts. Go calibrates
`b.N` independently for each engine unless `-benchtime=Nx` is supplied.
Create a benchmark root on the filesystem being measured, then run sequential
writes with exactly 100,000 records per engine:

```sh
mkdir -p .benchmark-data
go test -run '^$' \
  -bench '^BenchmarkComparison$/^PutSequentialCompactionDisabled$' \
  -benchmem -benchtime=100000x -count=5 \
  -args -storage-root="$PWD/.benchmark-data"
```

Each batch iteration contains 100 records, so 1,000 iterations also process
exactly 100,000 records per engine:

```sh
go test -run '^$' \
  -bench '^BenchmarkComparison$/^BatchPutUnique100CompactionDisabled$' \
  -benchmem -benchtime=1000x -count=5 \
  -args -storage-root="$PWD/.benchmark-data"
```

Read-only workloads operate on a fixed fixture and can use duration-based
calibration:

```sh
go test -run '^$' \
  -bench '^BenchmarkComparison$/^(GetMemtable|GetPersistedWarm|ScanPersistedWarm100)$' \
  -benchmem -benchtime=3s -count=5 \
  -args -storage-root="$PWD/.benchmark-data"
```

Redirect a controlled command's output to a file to collect input for
`benchstat`, for example by appending `> baseline.txt`.

The optional `-seed` test argument overrides the canonical seed. The default
seed is `0x5eed687970686164` and dataset version is 1. Keep both unchanged for
historical comparisons. When overriding the seed, record it with the command:
pass `-seed=123456789` after `-args` alongside `-storage-root` in any command
above.

Benchmark directories are created as unique children of `-storage-root` and
removed after each benchmark. The supplied root itself is retained.

## Comparison contract

- Both engines receive the same deterministic keys, per-record values, and
  operation order generated from the benchmark seed.
- Native key representations are generated before timing: strings for HyphaDB
  and byte slices for Pebble.
- Both engines keep the WAL enabled.
- Async writes do not request an `fsync`; sync batch commits request one sync
  per 100 records.
- Write-path workloads disable automatic compaction and use oversized
  memtables to reduce flush interference. An engine can still flush if a run
  exceeds its active memtable.
- Both engines use a 64 MiB block cache.
- Persisted-read fixtures contain the same 10,000 records and are explicitly
  warmed before timing.
- Pebble point and iterator values are copied before their borrowed storage is
  released, matching HyphaDB's owned-value public API.
- Database creation, fixture loading, key generation, cache warming, closing,
  and reopening are outside timed regions.
- Write and scan results report `records/op`, `records/s`, and `total-records`
  so fixed-work comparisons can be verified from the output.

These are operation-level comparisons, not complete production workload
claims. Engines still use their own table formats, compression behavior,
Bloom implementations, and background architecture. Sustained compaction,
large datasets, mixed workloads, and latency distributions belong in the next
benchmark layer.
