"""Scenario 1: run one realistic query battery at every threshold-bracketing size.

This is the scenario that would have caught ESQL-40 and ESQL-41 before release: the
same shapes are replayed at 1, 2, 127, 128, 129, 255, 256, 257, ... rows, so a bug
that only appears once a byte boundary, a join strategy switch, or a spill partition
is crossed shows up as a divergence from real MySQL.
"""

from harness import SIZES, Differ, main


def battery(d: Differ, n: int) -> None:
    # `g` deliberately has few distinct values (grouping), `k` is unique (1:1 joins),
    # `s` mixes case and non-ASCII (collation), `nul` exercises NULL handling.
    d.ddl(
        "DROP TABLE IF EXISTS a",
        "DROP TABLE IF EXISTS b",
        "CREATE TABLE a (k INT PRIMARY KEY, g INT, s VARCHAR(32), nul INT, f DOUBLE)",
        "CREATE TABLE b (k INT PRIMARY KEY, g INT, s VARCHAR(32))",
    )
    words = ["ape", "Ape", "BEAR", "bear", "cat", "Ærlig", "ærlig", "zz"]
    rows_a, rows_b = [], []
    for i in range(1, n + 1):
        w = words[i % len(words)]
        nul = "NULL" if i % 7 == 0 else str(i % 13)
        rows_a.append(f"({i},{i % 8},'{w}',{nul},{i * 1.5})")
        rows_b.append(f"({i},{i % 5},'{words[(i + 3) % len(words)]}')")
    for tbl, rows in (("a", rows_a), ("b", rows_b)):
        for lo in range(0, len(rows), 500):
            chunk = ",".join(rows[lo : lo + 500])
            d.ddl(f"INSERT INTO {tbl} VALUES {chunk}")

    q = [
        # --- joins: the shapes where the key-encoding bug lived -----------------
        "SELECT COUNT(*) FROM a JOIN b ON a.k = b.k",
        "SELECT COUNT(*) FROM a JOIN b ON a.k = b.k WHERE a.k <> b.k",
        "SELECT COUNT(*) FROM a JOIN b ON a.g = b.g",
        "SELECT COUNT(*) FROM a JOIN b ON a.s = b.s",
        "SELECT COUNT(*) FROM a LEFT JOIN b ON a.k = b.k",
        "SELECT COUNT(*) FROM a LEFT JOIN b ON a.s = b.s",
        "SELECT COUNT(*) FROM a, b WHERE a.k = b.k",
        "SELECT SUM(a.k), MIN(a.k), MAX(a.k) FROM a JOIN b ON a.k = b.k",
        "SELECT COUNT(*) FROM a x JOIN a y ON x.k = y.k JOIN b z ON y.k = z.k",
        "SELECT a.g, COUNT(*) FROM a JOIN b ON a.k = b.k GROUP BY a.g ORDER BY a.g",
        # bare aggregate over a join: the 256-spurious-rows bug
        "SELECT COUNT(*) FROM a JOIN b ON a.k = b.k WHERE a.k < 0",
        "SELECT AVG(a.f) FROM a JOIN b ON a.k = b.k",
        # --- aggregation / grouping -------------------------------------------
        "SELECT COUNT(*), COUNT(nul), SUM(nul), MIN(nul), MAX(nul) FROM a",
        "SELECT g, COUNT(*), SUM(k), AVG(k) FROM a GROUP BY g ORDER BY g",
        "SELECT s, COUNT(*) FROM a GROUP BY s ORDER BY s, COUNT(*)",
        "SELECT COUNT(DISTINCT g), COUNT(DISTINCT s), COUNT(DISTINCT nul) FROM a",
        "SELECT g, COUNT(*) FROM a GROUP BY g HAVING COUNT(*) > 1 ORDER BY g",
        "SELECT SUM(k) FROM a WHERE nul IS NULL",
        "SELECT COUNT(*) FROM a WHERE nul IS NOT NULL",
        # --- ordering / paging (index-accelerated paths) -----------------------
        "SELECT k FROM a ORDER BY k LIMIT 5",
        "SELECT k FROM a ORDER BY k DESC LIMIT 5",
        "SELECT k FROM a ORDER BY g, k LIMIT 10",
        "SELECT k FROM a ORDER BY nul, k LIMIT 10",
        "SELECT k FROM a ORDER BY nul DESC, k LIMIT 10",
        "SELECT k, s FROM a ORDER BY s, k LIMIT 12",
        "SELECT k FROM a ORDER BY k LIMIT 5 OFFSET 100",
        "SELECT k FROM a WHERE g = 3 ORDER BY k DESC LIMIT 5",
        # --- DISTINCT ----------------------------------------------------------
        "SELECT DISTINCT g FROM a ORDER BY g",
        "SELECT DISTINCT s FROM a ORDER BY s",
        "SELECT COUNT(*) FROM (SELECT DISTINCT g, s FROM a) t",
        # --- subqueries --------------------------------------------------------
        "SELECT COUNT(*) FROM a WHERE k IN (SELECT k FROM b WHERE g = 2)",
        "SELECT COUNT(*) FROM a WHERE k NOT IN (SELECT k FROM b WHERE g = 2)",
        "SELECT COUNT(*) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
        "SELECT COUNT(*) FROM a WHERE k > (SELECT AVG(k) FROM a)",
        # --- string / pattern matching (collation-sensitive) -------------------
        "SELECT COUNT(*) FROM a WHERE s LIKE 'a%'",
        "SELECT COUNT(*) FROM a WHERE s LIKE '%r%'",
        "SELECT COUNT(*) FROM a WHERE s REGEXP '^[ab]'",
        "SELECT COUNT(*) FROM a WHERE s REGEXP 'EAR'",
        "SELECT COUNT(*) FROM a WHERE s = 'bear'",
        "SELECT COUNT(*) FROM a WHERE s > 'cat'",
        "SELECT s, COUNT(*) FROM a WHERE s IN ('ape','BEAR') GROUP BY s ORDER BY s",
        # --- window functions --------------------------------------------------
        "SELECT k, ROW_NUMBER() OVER (ORDER BY k) FROM a ORDER BY k LIMIT 8",
        "SELECT k, RANK() OVER (PARTITION BY g ORDER BY k) FROM a ORDER BY k LIMIT 8",
        "SELECT k, NTILE(4) OVER (ORDER BY k) FROM a ORDER BY k LIMIT 8",
        "SELECT k, SUM(k) OVER (PARTITION BY g) FROM a ORDER BY k LIMIT 8",
        "SELECT k, LAG(k) OVER (ORDER BY k), LEAD(k) OVER (ORDER BY k) FROM a ORDER BY k LIMIT 8",
        # --- CTE ---------------------------------------------------------------
        "WITH t AS (SELECT g, COUNT(*) c FROM a GROUP BY g) SELECT COUNT(*) FROM t WHERE c > 0",
        # --- arithmetic / NULL semantics ---------------------------------------
        "SELECT COUNT(*) FROM a WHERE nul + 1 > 2",
        "SELECT COUNT(*) FROM a WHERE nul IS NULL OR nul > 5",
        "SELECT SUM(k * 2), SUM(k) * 2 FROM a",
        "SELECT COUNT(*) FROM a WHERE k % 3 = 0",
    ]
    for sql in q:
        d.check(sql, label=f"[n={n}] {sql}")


def scenario() -> bool:
    ok = True
    for n in SIZES:
        d = Differ(db="s1")
        battery(d, n)
        ok &= d.report(f"n={n}")
    return ok


if __name__ == "__main__":
    raise SystemExit(main([("S1: threshold sweep vs MySQL 8.4", scenario)]))
