# Storage rewrite profiling

This note records the reproducible baseline and rejected prototypes for the
storage-heavy `ALTER TABLE ... ADD PRIMARY KEY` path. Run the workload with:

```sh
python3 bench/storage_rewrite.py --port 3307 --label ElyraSQL \
  --rows 20000 100000 500000 --indexes 1 --repeats 5
```

## Baseline

Release build, default full durability, Apple Silicon host, one secondary
index. Setup is excluded; every sample creates and populates a fresh table.

| Rows | Median | p95 | Samples |
|---:|---:|---:|---:|
| 20,000 | 216.92 ms | 224.86 ms | 3 |
| 100,000 | 1,275.94 ms | 3,660.31 ms | 3 |
| 500,000 | 7,479.01 ms | 8,482.13 ms | 3 |

At 20,000 rows, index count exposes the expected mutation amplification:

| Secondary indexes | Median | p95 | Samples |
|---:|---:|---:|---:|
| 0 | 75.09 ms | 76.30 ms | 5 |
| 1 | 182.45 ms | 209.96 ms | 5 |
| 2 | 352.78 ms | 355.71 ms | 5 |

The difference between the two 20,000-row runs is normal host/filesystem
variance; use same-run comparisons when evaluating a future implementation.

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

## MySQL comparison

The prior native MySQL 8.0.33 measurement for the 20,000-row operation was
45.34 ms. A local MySQL 8.4 Docker run was intentionally excluded from the
comparison because the available image was amd64-emulated on an arm64 host; its
383-802 ms results measured emulation overhead rather than engine performance.

## Recommended storage design

The promising next step is a generational/shadow keyspace:

1. Build the replacement table and indexes under a new physical generation in
   bounded commits while the current generation remains readable.
2. Validate the source generation or table write sequence.
3. Atomically switch the table catalog to the new generation with one small
   durable commit.
4. Replicate the logical generation switch, then reclaim the old generation
   asynchronously and resumably.

This removes the full-table delete/reinsert set from the atomic commit and also
addresses the rewrite's peak-memory problem. It requires a storage-format and
replication design, so it should be implemented separately with migration,
crash-recovery, snapshot, binlog, and cluster coverage.
