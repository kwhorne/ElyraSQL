"""Scenario 3: performance profile vs MySQL, and security enforcement.

Performance is measured as a *ratio* against a reference MySQL on the same host and
data, because absolute numbers say little on a shared laptop. The point is to find
operations where ElyraSQL is unexpectedly slower, not to win a benchmark.

Security checks assert that privileges are actually enforced per action, that
administrative statements are gated, and that hostile input is handled as data
rather than as SQL.
"""

from __future__ import annotations

import statistics
import sys
import time

import pymysql

import os

ELYRA = dict(
    host="127.0.0.1",
    port=int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("ELYRA_PORT", "3400")),
    user="root",
)
MYSQL = dict(
    host="127.0.0.1",
    port=int(os.environ.get("MYSQL_PORT", "3308")),
    user="root",
    password="root",
)
# Smaller by default in CI: the point of the perf section there is to spot an
# order-of-magnitude regression, not to produce publishable numbers on a shared
# runner.
N = int(os.environ.get("SCENARIO_ROWS", "200000"))


def timed(cur, sql: str, reps: int = 5) -> float:
    """Best-of-N wall time in ms (best, to reduce noise from other load)."""
    best = float("inf")
    for _ in range(reps):
        t = time.perf_counter()
        cur.execute(sql)
        cur.fetchall()
        best = min(best, (time.perf_counter() - t) * 1000)
    return best


def seed(cur, n: int) -> None:
    cur.execute("DROP TABLE IF EXISTS perf")
    cur.execute(
        "CREATE TABLE perf (id INT PRIMARY KEY, g INT, amt INT, s VARCHAR(32))"
    )
    batch = 2000
    for lo in range(1, n + 1, batch):
        vals = ",".join(
            f"({i},{i % 500},{(i * 7919) % 100000},'row-{i}')"
            for i in range(lo, min(lo + batch, n + 1))
        )
        cur.execute(f"INSERT INTO perf VALUES {vals}")
    cur.execute("CREATE INDEX ix_g ON perf (g)")
    cur.execute("CREATE INDEX ix_amt ON perf (amt)")


def performance() -> bool:
    print(f"\n[performance] ratio vs MySQL 8.4 on {N:,} rows (lower is better for us)")
    e = pymysql.connect(autocommit=True, **ELYRA)
    m = pymysql.connect(autocommit=True, **MYSQL)
    ec, mc = e.cursor(), m.cursor()
    mc.execute("DROP DATABASE IF EXISTS perfdb")
    mc.execute("CREATE DATABASE perfdb")
    mc.execute("USE perfdb")
    t = time.perf_counter()
    seed(ec, N)
    elyra_load = (time.perf_counter() - t) * 1000
    t = time.perf_counter()
    seed(mc, N)
    mysql_load = (time.perf_counter() - t) * 1000
    print(f"  {'bulk load':38} elyra {elyra_load:9.0f}ms  mysql {mysql_load:9.0f}ms"
          f"  ratio {elyra_load / mysql_load:5.2f}x")

    queries = [
        ("point lookup by PK", "SELECT * FROM perf WHERE id = 123456"),
        ("PK range scan (1k rows)", "SELECT SUM(amt) FROM perf WHERE id BETWEEN 1000 AND 2000"),
        ("indexed equality", "SELECT COUNT(*) FROM perf WHERE g = 250"),
        ("indexed range", "SELECT COUNT(*) FROM perf WHERE amt BETWEEN 100 AND 5000"),
        ("COUNT(*) whole table", "SELECT COUNT(*) FROM perf"),
        ("SUM whole table", "SELECT SUM(amt) FROM perf"),
        ("GROUP BY 500 groups", "SELECT g, COUNT(*), SUM(amt) FROM perf GROUP BY g"),
        ("GROUP BY + HAVING", "SELECT g, SUM(amt) FROM perf GROUP BY g HAVING SUM(amt) > 1000"),
        ("COUNT(DISTINCT)", "SELECT COUNT(DISTINCT g) FROM perf"),
        ("top-N by PK desc", "SELECT id FROM perf ORDER BY id DESC LIMIT 40"),
        ("top-N by indexed col", "SELECT id FROM perf ORDER BY amt LIMIT 40"),
        ("top-N deep offset", "SELECT id FROM perf ORDER BY id LIMIT 40 OFFSET 150000"),
        ("full sort (unindexed)", "SELECT id FROM perf ORDER BY s LIMIT 40"),
        ("self join on PK", "SELECT COUNT(*) FROM perf a JOIN perf b ON a.id = b.id"),
        ("join + group by", "SELECT a.g, COUNT(*) FROM perf a JOIN perf b ON a.id = b.id GROUP BY a.g"),
        ("LIKE prefix", "SELECT COUNT(*) FROM perf WHERE s LIKE 'row-1%'"),
        ("REGEXP scan", "SELECT COUNT(*) FROM perf WHERE s REGEXP '7$'"),
        ("scalar subquery", "SELECT COUNT(*) FROM perf WHERE amt > (SELECT AVG(amt) FROM perf)"),
        ("IN (subquery)", "SELECT COUNT(*) FROM perf WHERE g IN (SELECT g FROM perf WHERE amt < 100)"),
        ("window function", "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY amt) rn FROM perf) x WHERE rn <= 20"),
    ]
    slow = []
    for label, sql in queries:
        try:
            a = timed(ec, sql)
        except Exception as ex:
            print(f"  {label:38} elyra ERROR {ex.args[0]}")
            slow.append((label, "error"))
            continue
        b = timed(mc, sql)
        ratio = a / b if b > 0 else float("inf")
        flag = "  <-- slower" if ratio > 3 else ""
        print(f"  {label:38} elyra {a:9.2f}ms  mysql {b:9.2f}ms  ratio {ratio:5.2f}x{flag}")
        if ratio > 3:
            slow.append((label, f"{ratio:.1f}x"))
    if slow:
        print(f"\n  operations >3x slower than MySQL: {slow}")
    return True


def security() -> bool:
    print("\n[security] privilege enforcement and hostile input")
    ok = True

    def check(name: str, cond: bool, detail: str = "") -> None:
        nonlocal ok
        ok &= cond
        print(f"  {'OK  ' if cond else '*** '}{name}" + (f" -- {detail}" if detail else ""))

    admin = pymysql.connect(autocommit=True, **ELYRA)
    ac = admin.cursor()
    ac.execute("DROP TABLE IF EXISTS sec")
    ac.execute("CREATE TABLE sec (id INT PRIMARY KEY, secret VARCHAR(64))")
    ac.execute("INSERT INTO sec VALUES (1,'classified')")
    for u in ("reader", "writer"):
        try:
            ac.execute(f"DROP USER '{u}'")
        except Exception:
            pass
    ac.execute("CREATE USER 'reader' IDENTIFIED BY 'ReaderPw#2026'")
    ac.execute("CREATE USER 'writer' IDENTIFIED BY 'WriterPw#2026'")
    ac.execute("GRANT SELECT ON *.* TO 'reader'")
    ac.execute("GRANT SELECT, INSERT ON *.* TO 'writer'")

    def as_user(user: str, pw: str):
        return pymysql.connect(
            host=ELYRA["host"], port=ELYRA["port"], user=user, password=pw, autocommit=True
        )

    # --- authentication -----------------------------------------------------
    try:
        as_user("reader", "wrong")
        check("wrong password is rejected", False)
    except Exception as ex:
        check("wrong password is rejected", True, f"errno {ex.args[0]}")
    try:
        as_user("nosuchuser", "x")
        check("unknown user is rejected", False)
    except Exception as ex:
        check("unknown user is rejected", True, f"errno {ex.args[0]}")

    # --- per-action privileges ---------------------------------------------
    rc = as_user("reader", "ReaderPw#2026").cursor()
    rc.execute("SELECT secret FROM sec")
    check("granted SELECT works", rc.fetchone()[0] == "classified")
    for stmt, what in [
        ("INSERT INTO sec VALUES (2,'x')", "INSERT"),
        ("UPDATE sec SET secret='y' WHERE id=1", "UPDATE"),
        ("DELETE FROM sec WHERE id=1", "DELETE"),
        ("DROP TABLE sec", "DROP"),
        ("CREATE TABLE evil (x INT)", "CREATE"),
        ("GRANT ALL ON *.* TO 'reader'", "GRANT (privilege escalation)"),
        ("CREATE USER 'mallory' IDENTIFIED BY 'p'", "CREATE USER"),
    ]:
        try:
            rc.execute(stmt)
            check(f"read-only user refused {what}", False, "statement succeeded")
        except Exception as ex:
            check(f"read-only user refused {what}", True, f"errno {ex.args[0]}")

    wc = as_user("writer", "WriterPw#2026").cursor()
    wc.execute("INSERT INTO sec VALUES (3,'ok')")
    check("granted INSERT works for writer", True)
    for stmt, what in [
        ("UPDATE sec SET secret='z' WHERE id=3", "UPDATE"),
        ("DELETE FROM sec WHERE id=3", "DELETE"),
    ]:
        try:
            wc.execute(stmt)
            check(f"INSERT-only user refused {what}", False, "statement succeeded")
        except Exception as ex:
            check(f"INSERT-only user refused {what}", True, f"errno {ex.args[0]}")

    # --- administrative statements are gated -------------------------------
    for stmt, what in [
        ("BACKUP TO '/tmp/should-not-exist.edb'", "BACKUP"),
        ("LOAD DATA INFILE '/etc/passwd' INTO TABLE sec", "LOAD DATA INFILE"),
        ("PURGE BINARY LOGS BEFORE '2020-01-01'", "PURGE BINARY LOGS"),
    ]:
        try:
            rc.execute(stmt)
            check(f"non-admin refused {what}", False, "statement succeeded")
        except Exception as ex:
            check(f"non-admin refused {what}", True, f"errno {ex.args[0]}")
    import os

    check(
        "refused BACKUP wrote no file",
        not os.path.exists("/tmp/should-not-exist.edb"),
    )

    # --- hostile input is data, not SQL ------------------------------------
    payloads = [
        "'; DROP TABLE sec; --",
        "1' OR '1'='1",
        "\\'; DELETE FROM sec WHERE 1=1; --",
        "a\x00b",
        "'" * 50,
        "-- comment\nSELECT 1",
        "/*!32302 DROP TABLE sec */",
    ]
    ac.execute("DROP TABLE IF EXISTS inj")
    ac.execute("CREATE TABLE inj (id INT PRIMARY KEY, payload VARCHAR(200))")
    stored = 0
    for i, pl in enumerate(payloads, 1):
        try:
            ac.execute("INSERT INTO inj VALUES (%s, %s)", (i, pl))
            stored += 1
        except Exception as ex:
            print(f"       payload {i} rejected: errno {ex.args[0]}")
    ac.execute("SELECT COUNT(*) FROM inj")
    check("hostile strings stored as data", ac.fetchone()[0] == stored, f"{stored} payloads")
    ac.execute("SELECT COUNT(*) FROM sec")
    check("injection payloads did not affect other tables", ac.fetchone()[0] >= 1)
    # Round-trip fidelity: what went in must come out byte-identical.
    ac.execute("SELECT payload FROM inj WHERE id = 1")
    check("payload round-trips unchanged", ac.fetchone()[0] == payloads[0])

    # --- error messages should not leak internals --------------------------
    leaked = []
    for bad in ["SELECT * FROM nonexistent_table", "SELECT bad_col FROM sec", "SELECT 1/0", "SLECT 1"]:
        try:
            rc.execute(bad)
        except Exception as ex:
            msg = str(ex.args[1] if len(ex.args) > 1 else ex)
            for word in ("redb", "sqlparser", "panicked", "/Users/", "src/", ".rs:"):
                if word in msg:
                    leaked.append((bad, word))
    check("errors leak no internal names or paths", not leaked, str(leaked[:3]))
    return ok


if __name__ == "__main__":
    # Performance is informational: ratios on a shared CI runner are too noisy to
    # gate a build on. Security is a gate.
    p = True if os.environ.get("SKIP_PERF") else performance()
    s = security()
    print("\n  PERF/SEC SCENARIO: " + ("PASS" if (p and s) else "FAIL"))
    sys.exit(0 if (p and s) else 1)
