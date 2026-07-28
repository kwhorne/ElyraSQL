"""Differential scenario harness: run identical workloads against ElyraSQL and a
reference MySQL, and compare results exactly.

Why this exists: two wrong-result bugs shipped in 1.4.12 (ESQL-40, ESQL-41) because
every existing test sat *below* the internal thresholds where the bugs lived -- the
hash-join key only collided for integers >= 128, and the spurious aggregate rows
only appeared once the spill-partition path was used. The lesson is that a test
battery must deliberately straddle every internal threshold, not just exercise
"typical" data.

`SIZES` therefore brackets each known boundary, and scenarios are expected to run
the same query shapes at every one of them.
"""

from __future__ import annotations

import decimal
import sys
import traceback

import pymysql

# Overridable so the same battery can run against the container image or a local
# build under test: ELYRA_PORT / ELYRA_PASSWORD / MYSQL_PORT.
import os

ELYRA = dict(
    host="127.0.0.1",
    port=int(os.environ.get("ELYRA_PORT", "3307")),
    user="root",
    password=os.environ.get("ELYRA_PASSWORD", "elyra"),
)
MYSQL = dict(
    host="127.0.0.1",
    port=int(os.environ.get("MYSQL_PORT", "3308")),
    user="root",
    password="root",
)

# Row counts that bracket internal thresholds, so a query shape is exercised on
# both sides of each one:
#   128/256  - byte boundaries in key encoding (ESQL-40) and spill partitions (ESQL-41)
#   2048     - NLJ_MAX_DRIVING and MERGE_MIN (index-NLJ vs hash vs merge join)
#   4096     - scan_batch size
#   8192     - parallel_aggregate batch size
SIZES = [1, 2, 127, 128, 129, 255, 256, 257, 1023, 2047, 2048, 2049, 4095, 4097, 8193]


def connect(cfg: dict, db: str | None = None) -> pymysql.Connection:
    c = pymysql.connect(autocommit=True, **cfg)
    if db:
        cur = c.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        cur.execute(f"USE {db}")
    return c


def normalise(rows) -> list:
    """Compare values, ignoring representational differences that are not bugs.

    MySQL returns SUM/AVG of integers as DECIMAL and ElyraSQL as an integer/float;
    that is a type-metadata difference, not a wrong answer, so numbers are compared
    by value. Everything else is compared exactly.
    """
    out = []
    for row in rows:
        vals = []
        for v in row:
            if isinstance(v, decimal.Decimal):
                v = float(v)
            elif isinstance(v, (bytes, bytearray)):
                v = bytes(v)
            elif isinstance(v, int) and not isinstance(v, bool):
                v = float(v)
            elif isinstance(v, float):
                v = round(v, 9)
            vals.append(v)
        out.append(tuple(vals))
    return out


def run_one(cur, sql: str):
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return ("ok", normalise(rows))
    except Exception as e:  # noqa: BLE001 - any client/server error is a datapoint
        code = e.args[0] if e.args else "?"
        return ("err", code)


class Differ:
    """Runs SQL on both engines and records divergences."""

    def __init__(self, db: str = "scen"):
        self.e = connect(ELYRA)
        self.m = connect(MYSQL)
        self.ec, self.mc = self.e.cursor(), self.m.cursor()
        # Fresh, identically named schema on both sides.
        for cur, drop in ((self.ec, True), (self.mc, True)):
            if drop:
                try:
                    cur.execute(f"DROP DATABASE IF EXISTS {db}")
                except Exception:
                    pass
        for cur in (self.ec, self.mc):
            cur.execute(f"CREATE DATABASE {db}")
            cur.execute(f"USE {db}")
        self.passed = 0
        self.diverged: list[tuple[str, object, object]] = []
        self.errors: list[tuple[str, object, object]] = []

    def ddl(self, *stmts: str) -> None:
        """Statements that must succeed identically on both sides."""
        for s in stmts:
            for cur, name in ((self.ec, "elyra"), (self.mc, "mysql")):
                try:
                    cur.execute(s)
                except Exception as ex:
                    print(f"  SETUP FAILED on {name}: {s[:80]}\n    {ex}")
                    raise

    def check(self, sql: str, label: str = "") -> bool:
        """Compare one query. Errors are compared as 'both failed' vs 'one failed'."""
        a = run_one(self.ec, sql)
        b = run_one(self.mc, sql)
        tag = label or sql
        if a[0] == "err" and b[0] == "err":
            # Both refused: acceptable (codes may differ; message text is not
            # part of the contract we are testing here).
            self.passed += 1
            return True
        if a[0] != b[0]:
            self.errors.append((tag, a, b))
            return False
        if a[1] != b[1]:
            self.diverged.append((tag, a[1], b[1]))
            return False
        self.passed += 1
        return True

    def report(self, title: str) -> bool:
        total = self.passed + len(self.diverged) + len(self.errors)
        print(f"\n  {title}: {self.passed}/{total} identical")
        for tag, ours, theirs in self.diverged[:12]:
            print(f"    DIVERGE {tag}")
            print(f"      elyra: {str(ours)[:150]}")
            print(f"      mysql: {str(theirs)[:150]}")
        for tag, ours, theirs in self.errors[:12]:
            print(f"    ERROR-MISMATCH {tag}")
            print(f"      elyra: {str(ours)[:150]}")
            print(f"      mysql: {str(theirs)[:150]}")
        ok = not self.diverged and not self.errors
        print(f"  {'PASS' if ok else 'FAIL'} - {len(self.diverged)} diverged, "
              f"{len(self.errors)} error-mismatches")
        return ok


def main(scenarios) -> int:
    failures = 0
    for name, fn in scenarios:
        print(f"\n{'=' * 72}\n{name}\n{'=' * 72}")
        try:
            if not fn():
                failures += 1
        except Exception:
            failures += 1
            traceback.print_exc()
    print(f"\n{'=' * 72}")
    print("ALL SCENARIOS PASSED" if failures == 0 else f"{failures} SCENARIO(S) FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit("harness is imported by scenario scripts, not run directly")
