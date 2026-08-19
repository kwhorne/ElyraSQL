#!/usr/bin/env python3
"""Benchmark storage-heavy ALTER TABLE ADD PRIMARY KEY rewrites.

Run against ElyraSQL or MySQL through their shared wire protocol. Each sample
uses a fresh table so setup and ALTER timing remain separate.
"""

import argparse
import math
import subprocess
import statistics
import threading
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


def process_rss_mib(pid):
    output = subprocess.check_output(
        ["ps", "-o", "rss=", "-p", str(pid)], text=True
    ).strip()
    return int(output) / 1024


def sample(cursor, rows, indexes, batch_rows, repeats, server_pid):
    timings = []
    peak_rss = []
    rss_growth = []
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
        baseline_rss = process_rss_mib(server_pid) if server_pid else None
        stop_sampling = threading.Event()
        observed_rss = [baseline_rss] if baseline_rss is not None else []

        def sample_rss():
            while not stop_sampling.wait(0.01):
                try:
                    observed_rss.append(process_rss_mib(server_pid))
                except (OSError, subprocess.SubprocessError, ValueError):
                    return

        sampler = None
        if server_pid:
            sampler = threading.Thread(target=sample_rss, daemon=True)
            sampler.start()
        started = time.perf_counter_ns()
        try:
            cursor.execute(f"ALTER TABLE {table} ADD PRIMARY KEY (id)")
            timings.append((time.perf_counter_ns() - started) / 1_000_000)
        finally:
            stop_sampling.set()
            if sampler:
                sampler.join()
        if observed_rss:
            peak = max(observed_rss)
            peak_rss.append(peak)
            rss_growth.append(peak - baseline_rss)
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        stored = cursor.fetchone()[0]
        if stored != rows:
            raise RuntimeError(f"rewrite retained {stored} of {rows} rows")
        cursor.execute(f"DROP TABLE {table}")
    return timings, peak_rss, rss_growth


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
    parser.add_argument(
        "--server-pid",
        type=int,
        help="sample this server process's foreground peak RSS during ALTER",
    )
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
    print(
        f"{'rows':>10} {'indexes':>8} {'median ms':>12} {'p95 ms':>12} "
        f"{'peak MiB':>10} {'RSS +MiB':>10} {'samples':>8}"
    )
    print("-" * 80)
    for rows in args.rows:
        for indexes in args.indexes:
            timings, peak_rss, rss_growth = sample(
                cursor,
                rows,
                indexes,
                args.batch_rows,
                args.repeats,
                args.server_pid,
            )
            peak = f"{statistics.median(peak_rss):.1f}" if peak_rss else "-"
            growth = f"{statistics.median(rss_growth):.1f}" if rss_growth else "-"
            print(
                f"{rows:>10,} {indexes:>8} {statistics.median(timings):>12.2f} "
                f"{percentile(timings, 0.95):>12.2f} {peak:>10} {growth:>10} "
                f"{len(timings):>8}"
            )

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
