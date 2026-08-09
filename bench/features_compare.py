#!/usr/bin/env python3
"""Compare recently added SQL paths through one persistent MySQL connection."""

import argparse
import math
import statistics
import tempfile
import time
from pathlib import Path

import pymysql


def sample(cur, sql, repeats):
    for _ in range(3):
        cur.execute(sql)
        cur.fetchall()
    times = []
    result = None
    for _ in range(repeats):
        started = time.perf_counter_ns()
        cur.execute(sql)
        result = cur.fetchall()
        times.append((time.perf_counter_ns() - started) / 1_000_000)
    ordered = sorted(times)
    p95 = ordered[min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)]
    return statistics.median(times), p95, result


def batches(cur, table, rows, render, size=1000):
    for start in range(0, rows, size):
        values = ",".join(render(i) for i in range(start, min(rows, start + size)))
        cur.execute(f"INSERT INTO {table} VALUES {values}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--database", default="")
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--batch-rows", type=int, default=1_000)
    parser.add_argument("--load-data", action="store_true")
    args = parser.parse_args()
    if args.rows <= 0 or args.batch_rows <= 0:
        parser.error("--rows and --batch-rows must be positive")

    conn = pymysql.connect(
        host="127.0.0.1", port=args.port, user="root", password="", autocommit=True
    )
    cur = conn.cursor()
    if args.database:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {args.database}")
        cur.execute(f"USE {args.database}")

    for table in (
        "feature_items",
        "feature_users",
        "feature_orders",
        "feature_rekey",
        "feature_load",
    ):
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(
        "CREATE TABLE feature_items ("
        "id BIGINT PRIMARY KEY, tenant BIGINT NOT NULL, created BIGINT NOT NULL, "
        "grp BIGINT, val BIGINT, label VARCHAR(32), "
        "INDEX tenant_created (tenant, created))"
    )
    cur.execute("CREATE TABLE feature_users (id BIGINT PRIMARY KEY, name VARCHAR(32))")
    cur.execute("CREATE TABLE feature_orders (id BIGINT PRIMARY KEY, user_id BIGINT)")

    started = time.perf_counter_ns()
    batches(
        cur,
        "feature_items",
        args.rows,
        lambda i: f"({i},{i % 100},{i},{i % 200},{i % 1000},'label{i % 1000}')",
        size=args.batch_rows,
    )
    insert_ms = (time.perf_counter_ns() - started) / 1_000_000
    batches(
        cur,
        "feature_users",
        args.rows,
        lambda i: f"({i},'user{i}')",
        size=args.batch_rows,
    )
    batches(
        cur,
        "feature_orders",
        args.rows,
        lambda i: f"({i},{(i * 17) % args.rows})",
        size=args.batch_rows,
    )
    cur.execute("CREATE INDEX orders_user ON feature_orders(user_id)")
    for table in ("feature_items", "feature_users", "feature_orders"):
        cur.execute(f"ANALYZE TABLE {table}")
        cur.fetchall()

    midpoint = args.rows // 2
    workloads = [
        ("PK point lookup", f"SELECT name FROM feature_users WHERE id={midpoint}", 100),
        (
            "composite prefix range",
            "SELECT COUNT(*) FROM feature_items "
            "WHERE tenant=42 AND created BETWEEN 10000 AND 40000",
            40,
        ),
        (
            "DISTINCT 1000 groups",
            "SELECT COUNT(*) FROM (SELECT DISTINCT label FROM feature_items) d",
            15,
        ),
        (
            "correlated EXISTS",
            "SELECT COUNT(*) FROM feature_users u WHERE EXISTS "
            "(SELECT 1 FROM feature_orders o WHERE o.user_id=u.id)",
            10,
        ),
        (
            "selective indexed join",
            f"SELECT u.name,o.id FROM feature_users u JOIN feature_orders o "
            f"ON u.id=o.user_id WHERE u.id={midpoint}",
            50,
        ),
    ]

    # Keep the window input bounded: this exposes frame-algorithm scaling without
    # letting one quadratic implementation monopolize the benchmark machine.
    window_rows = min(args.rows, 5_000)
    workloads.append(
        (
            f"RANGE window ({window_rows} rows)",
            "SELECT SUM(running_sum) FROM ("
            "SELECT SUM(val) OVER (ORDER BY created RANGE BETWEEN 10 PRECEDING "
            f"AND CURRENT ROW) running_sum FROM feature_items WHERE id < {window_rows}) w",
            5,
        )
    )

    results = []
    for name, sql, repeats in workloads:
        median, p95, result = sample(cur, sql, repeats)
        results.append((name, median, p95, result))

    rekey_rows = min(args.rows, 20_000)
    cur.execute("CREATE TABLE feature_rekey (id BIGINT, payload VARCHAR(32))")
    batches(
        cur,
        "feature_rekey",
        rekey_rows,
        lambda i: f"({i},'row{i}')",
        size=args.batch_rows,
    )
    started = time.perf_counter_ns()
    cur.execute("ALTER TABLE feature_rekey ADD PRIMARY KEY (id)")
    rekey_ms = (time.perf_counter_ns() - started) / 1_000_000

    load_ms = None
    load_error = None
    if args.load_data:
        cur.execute(
            "CREATE TABLE feature_load (id BIGINT PRIMARY KEY, payload VARCHAR(32))"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", prefix="elyra-load-", suffix=".tsv", delete=False
        ) as load_file:
            load_path = Path(load_file.name)
            for i in range(args.rows):
                load_file.write(f"{i}\trow{i}\n")
        try:
            started = time.perf_counter_ns()
            try:
                cur.execute(
                    f"LOAD DATA INFILE '{load_path}' INTO TABLE feature_load "
                    "FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'"
                )
                load_ms = (time.perf_counter_ns() - started) / 1_000_000
                cur.execute("SELECT COUNT(*) FROM feature_load")
                loaded = cur.fetchone()[0]
                if loaded != args.rows:
                    raise RuntimeError(f"LOAD DATA stored {loaded} of {args.rows} rows")
            except pymysql.MySQLError as error:
                load_error = str(error)
        finally:
            load_path.unlink(missing_ok=True)

    print(f"\n{args.label}: {args.rows:,} rows")
    print(f"{'workload':<34} {'median ms':>12} {'p95 ms':>12}")
    print("-" * 60)
    print(f"{'bulk insert feature_items':<34} {insert_ms:>12.2f} {insert_ms:>12.2f}")
    for name, median, p95, _ in results:
        print(f"{name:<34} {median:>12.2f} {p95:>12.2f}")
    print(f"{f'ADD PRIMARY KEY ({rekey_rows:,})':<34} {rekey_ms:>12.2f} {rekey_ms:>12.2f}")
    if load_ms is not None:
        print(f"{f'LOAD DATA ({args.rows:,})':<34} {load_ms:>12.2f} {load_ms:>12.2f}")
    elif load_error is not None:
        print(f"{'LOAD DATA':<34} {'unavailable':>12} {'unavailable':>12}")
        print(f"  {load_error}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
