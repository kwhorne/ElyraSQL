#!/usr/bin/env python3
"""OLAP benchmark: ElyraSQL vs ClickHouse (and MySQL/PostgreSQL for context).

Loads an identical, deterministic 1M-row analytical table into each engine
(each with its native optimal schema) and times a set of analytical queries:
full-table aggregation, low- and high-cardinality GROUP BY, top-N, and a
filtered aggregation.

Data is a pure function of the row number, so every engine holds the same rows:
    id, user_id = id % 10000, category = id % 100, amount = id % 1000

Usage:
    python3 bench/olap.py --rows 1000000
"""

import argparse
import statistics
import time
import urllib.parse
import urllib.request


def timed(fn, repeat=5):
    ts = []
    for _ in range(repeat):
        t = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - t) * 1000)
    return statistics.median(ts)


# ---- ClickHouse (HTTP) ------------------------------------------------------
class ClickHouse:
    label = "ClickHouse"

    def __init__(self, port=8124):
        self.base = f"http://127.0.0.1:{port}/"

    def q(self, sql):
        req = urllib.request.Request(self.base, data=sql.encode())
        return urllib.request.urlopen(req, timeout=300).read().decode()

    def load(self, n):
        self.q("DROP TABLE IF EXISTS events")
        self.q("CREATE TABLE events (id UInt64, user_id UInt64, category UInt64, "
               "amount UInt64) ENGINE = MergeTree ORDER BY id")
        self.q(f"INSERT INTO events SELECT number, number % 10000, number % 100, "
               f"number % 1000 FROM numbers({n})")
        self.q("OPTIMIZE TABLE events FINAL")

    def queries(self):
        return {
            "COUNT(*)": "SELECT count() FROM events",
            "global agg (SUM/AVG/MIN/MAX)": "SELECT sum(amount),avg(amount),min(amount),max(amount) FROM events",
            "GROUP BY category (100)": "SELECT category,count(),sum(amount) FROM events GROUP BY category FORMAT Null",
            "GROUP BY user_id top-10": "SELECT user_id,sum(amount) s FROM events GROUP BY user_id ORDER BY s DESC LIMIT 10",
            "filtered agg (amount>500)": "SELECT category,sum(amount) FROM events WHERE amount>500 GROUP BY category FORMAT Null",
        }

    def run(self, sql):
        return lambda: self.q(sql)


# ---- MySQL-family (ElyraSQL / MySQL / Percona) ------------------------------
class MySQLish:
    def __init__(self, label, port, user, password, database=None, elyra=False):
        self.label = label
        import pymysql
        self.c = pymysql.connect(host="127.0.0.1", port=port, user=user,
                                 password=password, autocommit=True)
        self.cur = self.c.cursor()
        if not elyra:
            self.cur.execute("CREATE DATABASE IF NOT EXISTS bench")
            self.cur.execute("USE bench")

    def load(self, n):
        self.cur.execute("DROP TABLE IF EXISTS events")
        self.cur.execute("CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT, "
                         "category BIGINT, amount BIGINT)")
        B = 10000
        for s in range(0, n, B):
            vals = ",".join(f"({i},{i%10000},{i%100},{i%1000})"
                            for i in range(s, min(s + B, n)))
            self.cur.execute(f"INSERT INTO events (id,user_id,category,amount) VALUES {vals}")

    def queries(self):
        return {
            "COUNT(*)": "SELECT COUNT(*) FROM events",
            "global agg (SUM/AVG/MIN/MAX)": "SELECT SUM(amount),AVG(amount),MIN(amount),MAX(amount) FROM events",
            "GROUP BY category (100)": "SELECT category,COUNT(*),SUM(amount) FROM events GROUP BY category",
            "GROUP BY user_id top-10": "SELECT user_id,SUM(amount) s FROM events GROUP BY user_id ORDER BY s DESC LIMIT 10",
            "filtered agg (amount>500)": "SELECT category,SUM(amount) FROM events WHERE amount>500 GROUP BY category",
        }

    def run(self, sql):
        def go():
            self.cur.execute(sql)
            self.cur.fetchall()
        return go


# ---- PostgreSQL -------------------------------------------------------------
class Postgres:
    label = "PostgreSQL"

    def __init__(self, port=5432):
        import psycopg2
        self.c = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                                  password="postgres", dbname="postgres")
        self.c.autocommit = True
        self.cur = self.c.cursor()

    def load(self, n):
        self.cur.execute("DROP TABLE IF EXISTS events")
        self.cur.execute("CREATE TABLE events (id BIGINT PRIMARY KEY, user_id BIGINT, "
                         "category BIGINT, amount BIGINT)")
        self.cur.execute(
            "INSERT INTO events SELECT g, g %% 10000, g %% 100, g %% 1000 "
            "FROM generate_series(0, %s) g", (n - 1,))

    def queries(self):
        return MySQLish.queries(self)

    def run(self, sql):
        def go():
            self.cur.execute(sql)
            self.cur.fetchall()
        return go


def bench_engine(eng, n):
    print(f"\n== {eng.label}: loading {n:,} rows ...", flush=True)
    t = time.perf_counter()
    eng.load(n)
    print(f"   loaded in {time.perf_counter()-t:.1f}s", flush=True)
    out = {}
    for name, sql in eng.queries().items():
        out[name] = timed(eng.run(sql))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--engines", default="elyra,clickhouse,mysql,postgres")
    a = ap.parse_args()

    factories = {
        "elyra": lambda: MySQLish("ElyraSQL", 3310, "root", "elyra", elyra=True),
        "clickhouse": lambda: ClickHouse(8124),
        "mysql": lambda: MySQLish("MySQL", 3308, "root", "root"),
        "percona": lambda: MySQLish("Percona", 3309, "root", "root"),
        "postgres": lambda: Postgres(5432),
    }
    results = {}
    names = None
    for key in a.engines.split(","):
        eng = factories[key.strip()]()
        results[eng.label] = bench_engine(eng, a.rows)
        names = list(results[eng.label].keys())

    print("\n" + "=" * 78)
    print(f"OLAP benchmark — {a.rows:,} rows (medians, ms; lower is better)")
    print("=" * 78)
    labels = list(results.keys())
    print(f"{'query':<32}" + "".join(f"{l:>12}" for l in labels))
    print("-" * 78)
    for q in names:
        row = "".join(f"{results[l][q]:>12.2f}" for l in labels)
        print(f"{q:<32}{row}")
    print("=" * 78)


if __name__ == "__main__":
    main()
