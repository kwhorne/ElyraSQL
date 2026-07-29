#!/usr/bin/env python3
"""Late-materialisation benchmark (ESQL-47 / ESQL-48 / ESQL-49).

Measures the two shapes where row-oriented paths used to materialise columns no
query reads:

* `ORDER BY ... LIMIT k` -- rows that lose the top-N admission test.
* Joins -- combined rows carrying every column of both sides.

Both are measured at two row *widths* (3 and 12 columns) because that is the
axis the defect scales on: if the fix works, the wide/narrow ratio flattens.
Run against ElyraSQL and against a reference MySQL on the same host:

    python3 bench/latemat.py --port 3307 --label ElyraSQL
    python3 bench/latemat.py --port 3308 --user root --password root --label MySQL
"""

import argparse
import statistics
import time

import pymysql

NARROW = 3
WIDE = 12


def cols(n, prefix="c"):
    """n TEXT payload columns -- TEXT is where a wasted copy costs an alloc."""
    return [f"{prefix}{i}" for i in range(n)]


def setup(cur, rows, width, table, distinct_g):
    payload = cols(width)
    defs = ", ".join(f"{c} VARCHAR(32)" for c in payload)
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cur.execute(f"CREATE TABLE {table} (k INT PRIMARY KEY, n INT, g INT, s VARCHAR(32), {defs})")
    batch = 2000
    words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]
    for lo in range(0, rows, batch):
        vals = []
        for i in range(lo, min(lo + batch, rows)):
            w = words[i % len(words)]
            pay = ",".join(f"'{w}{i % 97}'" for _ in payload)
            vals.append(f"({i},{(i * 7919) % rows},{i % distinct_g},'{w}{i % 1000}',{pay})")
        cur.execute(f"INSERT INTO {table} VALUES " + ",".join(vals))


def bench(cur, sql, repeat=5):
    times = []
    for _ in range(repeat):
        t = time.perf_counter()
        cur.execute(sql)
        cur.fetchall()
        times.append((time.perf_counter() - t) * 1000)
    return statistics.median(times)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3307)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--label", default="ElyraSQL")
    ap.add_argument("--rows", type=int, default=200_000)
    a = ap.parse_args()

    c = pymysql.connect(host=a.host, port=a.port, user=a.user,
                        password=a.password, autocommit=True)
    cur = c.cursor()
    try:
        cur.execute("CREATE DATABASE IF NOT EXISTS latemat")
        cur.execute("USE latemat")
    except pymysql.err.MySQLError:
        pass  # ElyraSQL has a single implicit database

    n = a.rows
    print(f"{a.label}: loading {n} rows x2 widths ...", flush=True)
    setup(cur, n, NARROW, "an", 1000)
    setup(cur, n, WIDE, "aw", 1000)
    setup(cur, n, NARROW, "bn", 1000)
    setup(cur, n, WIDE, "bw", 1000)

    shapes = [
        # ORDER BY ... LIMIT: only k rows are needed, at both widths.
        ("order int  LIMIT 100  narrow", "SELECT * FROM an ORDER BY n LIMIT 100"),
        ("order int  LIMIT 100  wide", "SELECT * FROM aw ORDER BY n LIMIT 100"),
        ("order text LIMIT 100  narrow", "SELECT * FROM an ORDER BY s LIMIT 100"),
        ("order text LIMIT 100  wide", "SELECT * FROM aw ORDER BY s LIMIT 100"),
        ("order int  LIMIT 100  +WHERE", "SELECT * FROM aw WHERE g < 500 ORDER BY n LIMIT 100"),
        ("order int  OFFSET 50  wide", "SELECT * FROM aw ORDER BY n LIMIT 100 OFFSET 50"),
        # Baselines that must not regress.
        ("order int  no limit   wide", "SELECT * FROM aw ORDER BY n"),
        ("scan COUNT(*)         wide", "SELECT COUNT(*) FROM aw"),
        ("scan SUM(n)           wide", "SELECT SUM(n) FROM aw"),
        # Joins: COUNT(*) reads no column of either side.
        ("join 1:1 COUNT(*)     narrow", "SELECT COUNT(*) FROM an JOIN bn ON an.k = bn.k"),
        ("join 1:1 COUNT(*)     wide", "SELECT COUNT(*) FROM aw JOIN bw ON aw.k = bw.k"),
        ("join 1:1 SUM(one col) narrow", "SELECT SUM(an.n) FROM an JOIN bn ON an.k = bn.k"),
        ("join 1:1 SUM(one col) wide", "SELECT SUM(aw.n) FROM aw JOIN bw ON aw.k = bw.k"),
        ("join ORDER BY LIMIT   wide",
         "SELECT aw.k, bw.n FROM aw JOIN bw ON aw.k = bw.k ORDER BY bw.n LIMIT 100"),
        # Fanout axis (ESQL-50): the 1:1 joins above pay one hash *key* per
        # emitted row, so per-key and per-emitted-row costs are indistinguishable.
        # Joining on `g` (1000 distinct values over n rows) keeps the same key
        # count while multiplying the emitted rows, which separates the two.
        ("join 1:N COUNT(*)     narrow", "SELECT COUNT(*) FROM an JOIN bn ON an.g = bn.g"),
        ("join 1:N COUNT(*)     wide", "SELECT COUNT(*) FROM aw JOIN bw ON aw.g = bw.g"),
        ("join 1:N SUM(one col) wide", "SELECT SUM(aw.n) FROM aw JOIN bw ON aw.g = bw.g"),
    ]

    print(f"\n{a.label} — {n} rows (median of 5, ms)\n")
    for name, sql in shapes:
        try:
            print(f"  {name:30s} {bench(cur, sql):8.1f}", flush=True)
        except Exception as e:  # a shape an engine rejects shouldn't kill the run
            print(f"  {name:30s}    ERROR {str(e)[:60]}", flush=True)


if __name__ == "__main__":
    main()
