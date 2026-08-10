#!/usr/bin/env python3
"""Benchmark storage-heavy ALTER TABLE ADD PRIMARY KEY rewrites.

Run against ElyraSQL or MySQL through their shared wire protocol. Each sample
uses a fresh table so setup and ALTER timing remain separate.
"""

import argparse
import math
import statistics
import time

import pymysql


def percentile(samples, fraction):
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]


def insert_rows(cursor, table, rows, batch_rows):
    for start in range(0, rows, batch_rows):
        values = ",".join(
            f"({row},{row % 1000},'payload-{row % 10000}')"
            for row in range(start, min(rows, start + batch_rows))
        )
        cursor.execute(f"INSERT INTO {table} VALUES {values}")


def sample(cursor, rows, indexes, batch_rows, repeats):
    timings = []
    for repetition in range(repeats):
        table = f"storage_rewrite_{repetition}"
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        index_sql = ""
        if indexes >= 1:
            index_sql += ", INDEX grp_idx(grp)"
        if indexes >= 2:
            index_sql += ", INDEX payload_idx(payload)"
        cursor.execute(
            f"CREATE TABLE {table} (id BIGINT, grp BIGINT, payload VARCHAR(32){index_sql})"
        )
        insert_rows(cursor, table, rows, batch_rows)
        started = time.perf_counter_ns()
        cursor.execute(f"ALTER TABLE {table} ADD PRIMARY KEY (id)")
        timings.append((time.perf_counter_ns() - started) / 1_000_000)
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        stored = cursor.fetchone()[0]
        if stored != rows:
            raise RuntimeError(f"rewrite retained {stored} of {rows} rows")
        cursor.execute(f"DROP TABLE {table}")
    return timings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="")
    parser.add_argument("--rows", type=int, nargs="+", default=[20_000, 100_000, 500_000])
    parser.add_argument("--indexes", type=int, nargs="+", default=[0, 1, 2])
    parser.add_argument("--batch-rows", type=int, default=1_000)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if min(args.rows) <= 0 or args.batch_rows <= 0 or args.repeats <= 0:
        parser.error("row counts, batch size, and repeats must be positive")
    if min(args.indexes) < 0 or max(args.indexes) > 2:
        parser.error("--indexes values must be between 0 and 2")

    connection = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        autocommit=True,
    )
    cursor = connection.cursor()
    if args.database:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {args.database}")
        cursor.execute(f"USE {args.database}")

    print(f"\n{args.label}: ALTER TABLE ADD PRIMARY KEY")
    print(f"{'rows':>10} {'indexes':>8} {'median ms':>12} {'p95 ms':>12} {'samples':>8}")
    print("-" * 56)
    for rows in args.rows:
        for indexes in args.indexes:
            timings = sample(cursor, rows, indexes, args.batch_rows, args.repeats)
            print(
                f"{rows:>10,} {indexes:>8} {statistics.median(timings):>12.2f} "
                f"{percentile(timings, 0.95):>12.2f} {len(timings):>8}"
            )

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
