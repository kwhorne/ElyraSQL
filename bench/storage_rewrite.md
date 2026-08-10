# Storage rewrite profiling

This note records the reproducible baseline and the implemented
shadow-generation rewrite for `ALTER TABLE ... ADD PRIMARY KEY`. Run the
workload with:

```sh
python3 bench/storage_rewrite.py --port 3307 --label ElyraSQL \
  --rows 20000 100000 500000 --indexes 1 --repeats 3 \
  --server-pid $(pgrep -n elyrasql)
```

## Baseline

Release build, default full durability, Apple Silicon host, one secondary
index. Setup is excluded; every sample creates and populates a fresh table.

| Rows | Median | p95 | Samples |
|---:|---:|---:|---:|
| 20,000 | 202.51 ms | 208.17 ms | 3 |
| 100,000 | 1,157.44 ms | 1,167.75 ms | 3 |
| 500,000 | 6,362.16 ms | 6,668.53 ms | 3 |

At 20,000 rows, index count exposes the expected mutation amplification:

| Secondary indexes | Median | p95 | Samples |
|---:|---:|---:|---:|
| 0 | 75.09 ms | 76.30 ms | 5 |
| 1 | 182.45 ms | 209.96 ms | 5 |
| 2 | 352.78 ms | 355.71 ms | 5 |

The difference between separate 20,000-row runs is normal host/filesystem
variance; the shadow comparison below uses matched fresh-process runs.

## Profile

A debug timing pass attributed about 82% of the operation to redb's validated
write transaction. Scan/decode/key construction, transaction staging, point
validation, and serializable range gathering were individually smaller. The
baseline transaction performs an ordered point removal and insertion for every
old/new data and secondary-index entry.

Two redb 2.6 range-removal prototypes were measured and rejected:

| 20,000 rows, one index | Median | Versus 182.45 ms baseline |
|---|---:|---:|
| Ordered point deletes (baseline) | 182.45 ms | 1.00x |
| `Table::retain_in` | 1,563.69 ms | 8.57x slower |
| `Table::extract_from_if` | 1,565.52 ms | 8.58x slower |

Both APIs still walk and remove entries individually inside redb; they are not
subtree-drop primitives. Streaming serializable-range comparison was also
rejected after a 10-sample median of 218.60 ms, because it did not satisfy the
no-regression gate.

## Shadow-generation implementation

The engine now builds rows and indexes in a new physical generation using
bounded commits, validates the source table's write sequence and catalog,
atomically switches a small generation pointer, and reclaims the old generation
asynchronously. Cleanup markers are durable and resumed during startup. ALTERs
inside an explicit transaction and multi-operation ALTERs retain the original
atomic path.

The default build batch is 100,000 rows and can be changed with
`ELYRASQL_REWRITE_BATCH_ROWS` (clamped to 1-100,000). A 20,000-row batch reduced
memory further, but its extra durable commits made the 500,000-row workload
slightly slower than baseline. The 100,000-row default was the best measured
latency/memory balance.

Matched release builds, full durability, one secondary index, three samples per
cell:

| Rows | Baseline median | Shadow median | Change | Baseline RSS growth | Shadow RSS growth |
|---:|---:|---:|---:|---:|---:|
| 20,000 | 202.51 ms | 89.60 ms | 55.8% faster | 14.9 MiB | 5.8 MiB |
| 100,000 | 1,157.44 ms | 456.04 ms | 60.6% faster | 45.1 MiB | 46.9 MiB |
| 500,000 | 6,362.16 ms | 4,037.35 ms | 36.5% faster | 304.8 MiB | 201.6 MiB |

RSS is sampled from the server every 10 ms during the foreground ALTER and is
reported as growth from immediately before that ALTER. It excludes deferred
old-generation cleanup. Absolute median peaks were 99.1/73.7 MiB, 347.3/259.9
MiB, and 1,226.0/708.9 MiB for baseline/shadow respectively, though allocator
retention makes the per-operation growth the more useful comparison.

The improvement comes from replacing one full-table validated mutation set
with bounded generation-build commits and a small validated cutover. The cost
is temporary disk space for both generations and extra total I/O while the old
generation is reclaimed.

## MySQL comparison

The prior native MySQL 8.0.33 measurement for the same 20,000-row, one-index
operation was 45.34 ms. ElyraSQL improved from 202.51 ms (4.47x MySQL) to 89.60
ms (1.98x MySQL). A local MySQL 8.4 Docker run remains excluded because the
available image was amd64-emulated on an arm64 host; its 383-802 ms results
measured emulation overhead rather than native engine performance.
