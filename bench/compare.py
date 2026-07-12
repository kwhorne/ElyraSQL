#!/usr/bin/env python3
"""Cross-engine benchmark harness.

Runs an identical, portable SQL workload against ElyraSQL, MySQL, Percona
(MySQL wire) or PostgreSQL, and prints a summary table. Same schema, same rows,
same queries, same client machine — an honest apples-to-apples comparison of the
paths all four engines share (vector search is ElyraSQL-only and benchmarked
separately by bench/benchmark.py).

Usage:
    python3 bench/compare.py --driver mysql    --port 3307 --label ElyraSQL
    python3 bench/compare.py --driver mysql    --port 3308 --label MySQL --user root --password root
    python3 bench/compare.py --driver postgres --port 5432 --label Postgres --user postgres --password postgres
"""

import argparse
import random
import statistics
import time


def connect(a):
    if a.driver == "postgres":
        import psycopg2
        c = psycopg2.connect(host=a.host, port=a.port, user=a.user,
                             password=a.password, dbname=a.database)
        c.autocommit = True
        return c
    import pymysql
    # Connect without a schema first; ElyraSQL has a single implicit db, MySQL
    # needs one created/selected (done in main()).
    return pymysql.connect(host=a.host, port=a.port, user=a.user,
                           password=a.password, autocommit=True)


def bench(fn, repeat=1):
    times = []
    for _ in range(repeat):
        t = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t) * 1000)
    return statistics.median(times)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--driver", choices=["mysql", "postgres"], default="mysql")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3307)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--database", default="")
    ap.add_argument("--label", default="ElyraSQL")
    ap.add_argument("--rows", type=int, default=200_000)
    a = ap.parse_args()

    random.seed(0)
    c = connect(a)
    cur = c.cursor()
    N = a.rows
    results = []

    def exe(sql):
        cur.execute(sql)

    # MySQL/Percona need an explicit schema; ElyraSQL and Postgres already have
    # a usable default database.
    if a.driver == "mysql" and a.label.lower().find("elyra") < 0:
        db = a.database or "bench"
        exe(f"CREATE DATABASE IF NOT EXISTS {db}")
        exe(f"USE {db}")

    def q(sql):
        cur.execute(sql)
        cur.fetchall()

    # Schema (portable across MySQL and Postgres)
    for t in ("orders", "users"):
        try:
            exe(f"DROP TABLE IF EXISTS {t}")
        except Exception:
            pass
    exe("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, age BIGINT)")
    exe("CREATE TABLE orders (id BIGINT PRIMARY KEY, uid BIGINT, amount BIGINT)")

    # 1. Bulk insert throughput (batches of 2000, autocommit)
    def ins():
        for s in range(0, N, 2000):
            vals = ",".join(f"({i},'user{i}',{18 + i % 60})"
                            for i in range(s, min(s + 2000, N)))
            exe(f"INSERT INTO users (id,name,age) VALUES {vals}")
    dt = bench(ins)
    results.append((f"bulk insert {N:,} rows", dt, f"{N / (dt / 1000):,.0f} rows/s"))

    for s in range(0, N, 2000):
        vals = ",".join(f"({i},{i % N},{i % 1000})"
                        for i in range(s, min(s + 2000, N)))
        exe(f"INSERT INTO orders (id,uid,amount) VALUES {vals}")

    # 2. PK point lookup
    def pk():
        q(f"SELECT name FROM users WHERE id = {random.randint(0, N - 1)}")
    results.append(("PK point lookup", bench(pk, repeat=50), "clustered/PK"))

    # 3. Full scan + filter (no index)
    results.append(("full scan COUNT (no index)",
                    bench(lambda: q("SELECT COUNT(*) FROM users WHERE age = 42"), repeat=5),
                    f"{N:,} rows"))

    # 4. Secondary index lookup
    exe("CREATE INDEX idx_age ON users (age)")
    results.append(("indexed COUNT",
                    bench(lambda: q("SELECT COUNT(*) FROM users WHERE age = 42"), repeat=10),
                    "secondary index"))

    # 5. Aggregation GROUP BY
    results.append(("GROUP BY age",
                    bench(lambda: q("SELECT age, COUNT(*) FROM users GROUP BY age"), repeat=5),
                    "full aggregation"))

    # 6. Selective join (index nested-loop)
    exe("CREATE INDEX idx_uid ON orders (uid)")
    def joinq():
        uid = random.randint(0, N - 1)
        q(f"SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.uid WHERE u.id = {uid}")
    results.append(("selective join (index NLJ)", bench(joinq, repeat=50), "u.id = ?"))

    # 7. Range scan + ORDER BY + LIMIT
    results.append(("range ORDER BY LIMIT",
                    bench(lambda: q("SELECT id,name FROM users WHERE age BETWEEN 30 AND 40 ORDER BY id LIMIT 100"), repeat=10),
                    "top-100"))

    cur.close(); c.close()

    print("\n" + "=" * 72)
    print(f"{a.label}  ({a.driver} {a.host}:{a.port}, {N:,} rows)")
    print("=" * 72)
    print(f"{'workload':<34}{'median':>14}  note")
    print("-" * 72)
    for label, ms, note in results:
        val = f"{ms:,.2f} ms" if ms < 1000 else f"{ms / 1000:,.2f} s"
        print(f"{label:<34}{val:>14}  {note}")
    print("=" * 72)


if __name__ == "__main__":
    main()
