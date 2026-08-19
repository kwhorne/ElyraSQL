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

Matched release builds, full durability, one secondary index, five samples per
cell. The baseline is commit `c7cdebc`, immediately before the shadow-generation
work, and the comparison uses fresh server processes on the same Apple Silicon
host:

| Rows | Baseline median / p95 | Shadow median / p95 | Change | Baseline RSS growth | Shadow RSS growth |
|---:|---:|---:|---:|---:|---:|
| 20,000 | 203.01 / 206.95 ms | 100.57 / 126.58 ms | 50.5% faster | 6.6 MiB | 5.5 MiB |
| 100,000 | 1,219.81 / 1,545.40 ms | 525.75 / 736.08 ms | 56.9% faster | 44.8 MiB | 24.3 MiB |
| 500,000 | 6,287.03 / 6,809.33 ms | 3,930.00 / 6,086.40 ms | 37.5% faster | 269.7 MiB | 148.6 MiB |

RSS is sampled from the server every 10 ms during the foreground ALTER and is
reported as growth from immediately before that ALTER. It excludes deferred
old-generation cleanup. Absolute median peaks were 118.2/91.6 MiB,
382.6/280.5 MiB, and 1,374.3/739.5 MiB for baseline/shadow respectively, though
allocator retention makes the per-operation growth the more useful comparison.

The improvement comes from replacing one full-table validated mutation set
with bounded generation-build commits and a small validated cutover. The cost
is temporary disk space for both generations and extra total I/O while the old
generation is reclaimed.

## Native MySQL comparison

The same five-sample workload was also run against native MySQL 8.0.33 on the
same host. This is an engine comparison, not a claim of identical DDL semantics
or durability implementation.

| Rows | MySQL median / p95 | ElyraSQL baseline / MySQL | ElyraSQL shadow / MySQL |
|---:|---:|---:|---:|
| 20,000 | 40.05 / 47.58 ms | 5.07x | 2.51x |
| 100,000 | 127.17 / 135.26 ms | 9.59x | 4.13x |
| 500,000 | 563.86 / 699.20 ms | 11.15x | 6.97x |

The shadow rewrite closes roughly half the 20,000- and 100,000-row latency gap,
but MySQL remains substantially faster, especially as the table grows.

## Crash and sustained-load campaign

`storage_rewrite_stress.py` drives valid and invalid rewrites concurrently with
indexed reads, account transfers, and an atomic commit oracle. Its aggressive
mode adds randomized `SIGKILL`, `SIGSTOP` followed by `SIGKILL`, and repeated
kills during startup recovery. The five-minute campaign used:

```sh
python3 bench/storage_rewrite_stress.py \
  --data /tmp/elyra-shadow-chaos.edb \
  --log /tmp/elyra-shadow-chaos.log \
  --duration 300 --crash-min-ms 50 --crash-max-ms 300 \
  --crash-long-probability 0.15 --crash-long-max-ms 5000 \
  --startup-crash-probability 0.5 --max-startup-crashes 2 \
  --stop-before-kill-probability 0.25 --restart-attempts 3
```

It completed 327 forced crashes, including 237 additional recovery-time kills
and 90 stop-then-kill cycles. All 327 successful restarts passed the durable
invariants. Concurrent work included 1,272,209 indexed reads, 28,610 transfers,
7,184 atomic epochs, and 48 completed shadow rewrites. Peak RSS was 253.9 MiB
and the database peaked at 289.6 MiB.

An earlier run exposed one transient redb 2.6.3 startup panic after repeatedly
killing recovery (`assertion failed: !self.needs_recovery`). The preserved file
opened successfully on the next launch and no persistent corruption was found.
The retry-capable campaign records such startup failures rather than hiding
them. Torn-sector, reordered-write, and individual I/O-failure testing still
requires a lower-level fault-injection interface that redb does not expose.
