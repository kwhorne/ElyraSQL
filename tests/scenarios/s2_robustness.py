"""Scenario 2: robustness -- durability, concurrency invariants, and recovery.

A database is only a credible alternative if it keeps its promises when things go
wrong. Each check here is an invariant that must hold no matter how the server is
abused: committed data survives a hard kill, concurrent writers cannot lose or
duplicate money, and the server recovers from resource exhaustion instead of
degrading permanently.

Run against a *local* server (its process must be killable), not the container:
    python3 s2_robustness.py <port> <data-file> <binary>
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time

import pymysql

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 3400
DATA = sys.argv[2] if len(sys.argv) > 2 else "/tmp/rb.edb"
BIN = sys.argv[3] if len(sys.argv) > 3 else "./target/release/elyrasql"


def conn(**kw):
    return pymysql.connect(
        host="127.0.0.1", port=PORT, user="root", autocommit=True, **kw
    )


def start() -> subprocess.Popen:
    p = subprocess.Popen(
        [BIN, "serve", "--data", DATA, "--listen", f"127.0.0.1:{PORT}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(60):
        try:
            conn().close()
            return p
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("server did not come up")


def hard_kill(p: subprocess.Popen) -> None:
    """SIGKILL: no chance to flush, run destructors, or checkpoint."""
    os.kill(p.pid, signal.SIGKILL)
    p.wait()


results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"  {'OK  ' if ok else '*** '}{name}" + (f" -- {detail}" if detail else ""))


# ---------------------------------------------------------------- durability ----
def durability_after_hard_kill() -> None:
    """Every acknowledged commit must survive SIGKILL; nothing else may appear."""
    print("\n[durability] acknowledged commits survive SIGKILL")
    p = start()
    c = conn()
    cur = c.cursor()
    cur.execute("DROP TABLE IF EXISTS led")
    cur.execute("CREATE TABLE led (id INT PRIMARY KEY, v INT)")
    # Commit a known number of rows, one transaction each, and remember the last
    # id the server acknowledged.
    acked = 0
    for i in range(1, 501):
        cur.execute(f"INSERT INTO led VALUES ({i}, {i * 2})")
        acked = i
    hard_kill(p)

    p = start()
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT COUNT(*), SUM(v), MAX(id) FROM led")
    n, s, mx = cur.fetchone()
    check(
        "all acknowledged rows present after SIGKILL",
        n == acked and mx == acked,
        f"{n} rows, max id {mx}, expected {acked}",
    )
    check(
        "no phantom or corrupted rows",
        s == sum(i * 2 for i in range(1, acked + 1)),
        f"sum {s}",
    )
    # The file must still be fully usable for writes and reads.
    cur.execute("INSERT INTO led VALUES (99999, 1)")
    cur.execute("SELECT COUNT(*) FROM led")
    check("database writable after recovery", cur.fetchone()[0] == acked + 1)
    hard_kill(p)


def rollback_is_not_durable() -> None:
    """An uncommitted transaction must vanish on SIGKILL (atomicity)."""
    print("\n[durability] uncommitted work does not survive SIGKILL")
    p = start()
    c = conn()
    cur = c.cursor()
    cur.execute("DROP TABLE IF EXISTS atom")
    cur.execute("CREATE TABLE atom (id INT PRIMARY KEY, v INT)")
    cur.execute("INSERT INTO atom VALUES (1, 1)")
    # Open a transaction, write a lot, never commit.
    c2 = pymysql.connect(host="127.0.0.1", port=PORT, user="root", autocommit=False)
    k = c2.cursor()
    k.execute("BEGIN")
    for i in range(2, 400):
        k.execute(f"INSERT INTO atom VALUES ({i}, {i})")
    hard_kill(p)

    p = start()
    cur = conn().cursor()
    cur.execute("SELECT COUNT(*) FROM atom")
    n = cur.fetchone()[0]
    check("uncommitted rows absent after SIGKILL", n == 1, f"{n} rows, expected 1")
    hard_kill(p)


# --------------------------------------------------------------- concurrency ----
def concurrent_transfers_conserve_total() -> None:
    """Classic money-transfer invariant: the total must never change."""
    print("\n[concurrency] concurrent transfers conserve the total")
    p = start()
    cur = conn().cursor()
    cur.execute("DROP TABLE IF EXISTS acct")
    cur.execute("CREATE TABLE acct (id INT PRIMARY KEY, bal INT)")
    accounts, start_bal = 20, 1000
    cur.execute(
        "INSERT INTO acct VALUES "
        + ",".join(f"({i},{start_bal})" for i in range(accounts))
    )
    total_before = accounts * start_bal

    stop = threading.Event()
    errors: list[str] = []
    applied = [0]
    lock = threading.Lock()

    def worker(seed: int) -> None:
        import random

        rnd = random.Random(seed)
        try:
            c = pymysql.connect(
                host="127.0.0.1", port=PORT, user="root", autocommit=False
            )
        except Exception as e:  # noqa: BLE001
            errors.append(f"connect: {e}")
            return
        k = c.cursor()
        while not stop.is_set():
            a, b = rnd.randrange(accounts), rnd.randrange(accounts)
            if a == b:
                continue
            amt = rnd.randrange(1, 50)
            try:
                k.execute("BEGIN")
                k.execute(f"SELECT bal FROM acct WHERE id = {a}")
                bal_a = k.fetchone()[0]
                if bal_a < amt:
                    k.execute("ROLLBACK")
                    continue
                k.execute(f"UPDATE acct SET bal = bal - {amt} WHERE id = {a}")
                k.execute(f"UPDATE acct SET bal = bal + {amt} WHERE id = {b}")
                k.execute("COMMIT")
                with lock:
                    applied[0] += 1
            except Exception:
                # Serialisation conflicts are legitimate; the invariant is what matters.
                try:
                    k.execute("ROLLBACK")
                except Exception:
                    pass

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(8)]
    for t in threads:
        t.start()
    time.sleep(6)
    stop.set()
    for t in threads:
        t.join(timeout=10)

    cur = conn().cursor()
    cur.execute("SELECT SUM(bal), MIN(bal), COUNT(*) FROM acct")
    total_after, min_bal, n = cur.fetchone()
    check(
        "total conserved across concurrent transfers",
        total_after == total_before,
        f"{total_after} vs {total_before} after {applied[0]} commits",
    )
    check("no account went negative", min_bal >= 0, f"min balance {min_bal}")
    check("no rows lost", n == accounts, f"{n} accounts")
    check("no worker connection errors", not errors, "; ".join(errors[:3]))
    hard_kill(p)


def durability_under_concurrent_load() -> None:
    """Kill the server mid-write: the ledger must stay internally consistent."""
    print("\n[durability] SIGKILL during concurrent writes leaves a consistent file")
    p = start()
    cur = conn().cursor()
    cur.execute("DROP TABLE IF EXISTS pair")
    # Each transaction writes two rows that must both be present or both absent.
    cur.execute("CREATE TABLE pair (id INT PRIMARY KEY, tag INT, part INT)")

    stop = threading.Event()

    def writer(base: int) -> None:
        try:
            c = pymysql.connect(
                host="127.0.0.1", port=PORT, user="root", autocommit=False
            )
        except Exception:
            return
        k = c.cursor()
        i = base
        while not stop.is_set():
            try:
                k.execute("BEGIN")
                k.execute(f"INSERT INTO pair VALUES ({i * 2}, {i}, 0)")
                k.execute(f"INSERT INTO pair VALUES ({i * 2 + 1}, {i}, 1)")
                k.execute("COMMIT")
            except Exception:
                try:
                    k.execute("ROLLBACK")
                except Exception:
                    pass
            i += 100000

    threads = [threading.Thread(target=writer, args=(i,), daemon=True) for i in range(6)]
    for t in threads:
        t.start()
    time.sleep(4)
    hard_kill(p)  # kill while transactions are in flight
    stop.set()

    p = start()
    cur = conn().cursor()
    # Atomicity: every tag must have exactly 2 parts, never 1.
    cur.execute("SELECT COUNT(*) FROM (SELECT tag FROM pair GROUP BY tag HAVING COUNT(*) <> 2) x")
    broken = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM pair")
    total = cur.fetchone()[0]
    check(
        "no torn transactions after mid-write SIGKILL",
        broken == 0,
        f"{broken} half-written pairs out of {total} rows",
    )
    # And the file must accept new work.
    cur.execute("INSERT INTO pair VALUES (999999, 999999, 0)")
    check("writable after crash recovery", True)
    hard_kill(p)


# ------------------------------------------------------------------- recovery ----
def recovers_from_resource_exhaustion() -> None:
    """After hitting the join/connection caps, the server must be fully usable."""
    print("\n[recovery] server stays healthy after hitting resource limits")
    p = start()
    cur = conn().cursor()
    cur.execute("DROP TABLE IF EXISTS big")
    cur.execute("CREATE TABLE big (id INT PRIMARY KEY, v INT)")
    rows = ",".join(f"({i},{i % 7})" for i in range(1, 3001))
    cur.execute(f"INSERT INTO big VALUES {rows}")

    # Repeatedly trigger the join ceilings. A *cross* join no longer qualifies: those
    # stream now, so they complete instead of being refused (and would run for a very
    # long time at this size). A non-equi join is a shape the streaming chain declines,
    # so it materialises and the ceilings apply -- which is what this checks.
    refused = 0
    for _ in range(5):
        try:
            cur.execute(
                "SELECT COUNT(*) FROM big a JOIN big b ON a.v < b.v "
                "JOIN big c ON a.v < c.v JOIN big d ON a.v < d.v"
            )
            cur.fetchall()
        except Exception:
            refused += 1
    check("oversized joins are refused, not fatal", refused > 0, f"{refused}/5 refused")

    # The budget must be fully released: a legitimate join still works afterwards.
    cur.execute("SELECT COUNT(*) FROM big a JOIN big b ON a.id = b.id")
    n = cur.fetchone()[0]
    check("join budget reclaimed after refusals", n == 3000, f"got {n}")

    # Exhaust connections, then confirm capacity returns.
    held = []
    try:
        for _ in range(200):
            held.append(conn(connect_timeout=5))
    except Exception:
        pass
    got = len(held)
    for h in held:
        h.close()
    time.sleep(1)
    c2 = conn(connect_timeout=5)
    c2.cursor().execute("SELECT 1")
    check(
        "connection slots reclaimed after exhaustion",
        True,
        f"held {got} before refusal, reconnect OK",
    )
    hard_kill(p)


def main() -> int:
    for fn in (
        durability_after_hard_kill,
        rollback_is_not_durable,
        concurrent_transfers_conserve_total,
        durability_under_concurrent_load,
        recovers_from_resource_exhaustion,
    ):
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            check(f"{fn.__name__} raised", False, str(e)[:120])
    bad = [r for r in results if not r[1]]
    print(f"\n  {len(results) - len(bad)}/{len(results)} invariants held")
    if bad:
        for name, _, detail in bad:
            print(f"    FAILED: {name} -- {detail}")
    return 1 if bad else 0


if __name__ == "__main__":
    for f in (DATA, DATA + ".raftstate"):
        if os.path.exists(f):
            os.remove(f)
    sys.exit(main())
