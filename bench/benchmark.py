#!/usr/bin/env python3
"""ElyraSQL benchmark harness.

Runs a set of representative workloads against a running ElyraSQL server and
prints a summary table. Reproducible: point it at any ElyraSQL instance.

Usage:
    python3 bench/benchmark.py [--host 127.0.0.1] [--port 3307] \
        [--user root] [--password ''] [--rows 100000]
"""

import argparse
import random
import statistics
import time

import pymysql


def connect(a):
    return pymysql.connect(host=a.host, port=a.port, user=a.user,
                           password=a.password, autocommit=True)


def bench(label, fn, repeat=1):
    times = []
    result = None
    for _ in range(repeat):
        t = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t) * 1000)
    return label, statistics.median(times), result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3307)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--rows", type=int, default=100_000)
    a = ap.parse_args()

    random.seed(0)
    c = connect(a)
    cur = c.cursor()
    N = a.rows
    results = []

    # Schema
    cur.execute("DROP TABLE IF EXISTS users")
    cur.execute("DROP TABLE IF EXISTS orders")
    cur.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, age BIGINT)")
    cur.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, uid BIGINT, amount BIGINT)")

    # 1. Bulk insert throughput
    def ins():
        for s in range(0, N, 2000):
            vals = ",".join(f"({i},'user{i}',{18 + i % 60})" for i in range(s, min(s + 2000, N)))
            cur.execute(f"INSERT INTO users (id,name,age) VALUES {vals}")
    _, dt, _ = bench("bulk insert", ins)
    results.append((f"bulk insert {N:,} rows", dt, f"{N / (dt / 1000):,.0f} rows/s"))

    for s in range(0, N, 2000):
        vals = ",".join(f"({i},{i % N},{i % 1000})" for i in range(s, min(s + 2000, N)))
        cur.execute(f"INSERT INTO orders (id,uid,amount) VALUES {vals}")

    # 2. PK point lookup
    def pk():
        cur.execute(f"SELECT name FROM users WHERE id = {random.randint(0, N - 1)}")
        cur.fetchall()
    results.append((*bench("PK point lookup", pk, repeat=50)[:2], "clustered key"))

    # 3. Full scan + filter (no index)
    def scan():
        cur.execute("SELECT COUNT(*) FROM users WHERE age = 42")
        cur.fetchall()
    results.append((*bench("full scan COUNT (no index)", scan, repeat=5)[:2], f"{N:,} rows"))

    # 4. Secondary index lookup
    cur.execute("CREATE INDEX idx_age ON users (age)")
    def idx():
        cur.execute("SELECT COUNT(*) FROM users WHERE age = 42")
        cur.fetchall()
    results.append((*bench("indexed COUNT", idx, repeat=10)[:2], "secondary index"))

    # 5. Aggregation GROUP BY
    def agg():
        cur.execute("SELECT age, COUNT(*) FROM users GROUP BY age")
        cur.fetchall()
    results.append((*bench("GROUP BY age", agg, repeat=5)[:2], "full aggregation"))

    # 6. Selective join (index nested-loop)
    cur.execute("CREATE INDEX idx_uid ON orders (uid)")
    def joinq():
        uid = random.randint(0, N - 1)
        cur.execute(f"SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = {uid}")
        cur.fetchall()
    results.append((*bench("selective join (index NLJ)", joinq, repeat=50)[:2], "u.id = ?"))

    # 7. Vector ANN
    cur.execute("DROP TABLE IF EXISTS docs")
    cur.execute("CREATE TABLE docs (id BIGINT PRIMARY KEY, embedding VECTOR(32))")
    VN = min(N, 20000)
    for s in range(0, VN, 2000):
        vals = ",".join(
            "({},'[{}]')".format(i, ",".join(f"{random.random():.3f}" for _ in range(32)))
            for i in range(s, min(s + 2000, VN)))
        cur.execute(f"INSERT INTO docs (id,embedding) VALUES {vals}")
    cur.execute("CREATE INDEX idx_emb ON docs (embedding)")
    q = "[" + ",".join(f"{random.random():.3f}" for _ in range(32)) + "]"
    ann_sql = f"SELECT id FROM docs ORDER BY VEC_DISTANCE(embedding, '{q}') LIMIT 10"
    _, build_dt, _ = bench("vector ANN (first, builds HNSW)", lambda: cur.execute(ann_sql) or cur.fetchall())
    results.append((f"vector ANN build+query ({VN:,} vecs)", build_dt, "HNSW build"))
    results.append((*bench("vector ANN (cached)", lambda: cur.execute(ann_sql) or cur.fetchall(), repeat=20)[:2], "top-10"))

    cur.close(); c.close()

    # Report
    print("\n" + "=" * 72)
    print(f"ElyraSQL benchmark  ({a.host}:{a.port}, {N:,} rows)")
    print("=" * 72)
    print(f"{'workload':<40}{'median':>14}{'  note':<18}")
    print("-" * 72)
    for label, ms, note in results:
        val = f"{ms:,.2f} ms" if ms < 1000 else f"{ms / 1000:,.2f} s"
        print(f"{label:<40}{val:>14}  {note}")
    print("=" * 72)


if __name__ == "__main__":
    main()
