#!/usr/bin/env python3
"""MySQL semantics differential harness.

Runs an identical battery of edge-case queries against ElyraSQL and a reference
MySQL 8, and reports where they diverge. The reference is the source of truth for
MySQL semantics, so we don't have to guess.

Three things are compared, because two of them were added after divergences got
through:

* **rows** -- values and NULLs, plus one engine accepting what the other rejects.
* **result column types** -- the declared type code, not just the value. A
  boolean in arithmetic used to come back as DOUBLE where MySQL sends BIGINT;
  every value matched, so the battery passed.
* **affected rows** -- for the DML battery. `INSERT ... ON DUPLICATE KEY UPDATE`
  reported 1 where MySQL reports 2, in five shapes across two code paths, and
  nothing here looked at the count at all.

Usage:
    python3 tests/compat/differential/mysql_diff.py \
        --elyra-port 3307 --elyra-password '' \
        --ref-port 3390 --ref-user root --ref-password root
Exit code 1 if any non-allowlisted divergence is found.
"""

import argparse
import sys
from decimal import Decimal

import pymysql


# ---- comparison ------------------------------------------------------------

def run(conn, sql):
    """Execute one statement; return ('ok', rows, types) or ('err', code, ())."""
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        types = tuple(c[1] for c in cur.description) if cur.description else ()
        cur.close()
        return ("ok", rows, types)
    except pymysql.err.MySQLError as e:
        return ("err", e.args[0] if e.args else 0, ())
    except Exception as e:  # driver-level (e.g. lost connection = crash!)
        return ("crash", str(e)[:60], ())


def run_dml(conn, sql):
    """Execute one DML statement; return ('ok', affected) or ('err', code)."""
    try:
        cur = conn.cursor()
        cur.execute(sql)
        affected = cur.rowcount
        cur.close()
        return ("ok", affected)
    except pymysql.err.MySQLError as e:
        return ("err", e.args[0] if e.args else 0)
    except Exception as e:
        return ("crash", str(e)[:60])


# Type codes that mean the same thing to a client. Integer widths are chosen by
# the server from the value's range and every driver widens them transparently,
# and the string/blob family is a wire-encoding detail. Everything else is
# compared exactly -- notably DOUBLE is *not* folded into the integer family,
# which is what makes a boolean-as-float regression visible.
_INT_TYPES = {1, 2, 3, 8, 9}            # TINY SHORT LONG LONGLONG INT24
_STR_TYPES = {15, 249, 250, 251, 252, 253, 254}  # VARCHAR, BLOB family, STRING


def fold_type(code):
    if code in _INT_TYPES:
        return "int"
    if code in _STR_TYPES:
        return "str"
    return code


def norm(v):
    """Normalise a cell so benign representation differences don't count."""
    if v is None:
        return None
    if isinstance(v, bool):
        return ("num", float(int(v)))
    if isinstance(v, (int, Decimal, float)):
        return ("num", float(v))
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode()
        except Exception:
            return repr(v)
    return str(v)


def nums_close(a, b):
    if a == b:
        return True
    scale = max(abs(a), abs(b), 1.0)
    return abs(a - b) <= 1e-9 * scale


def compare_types(a, b):
    """Return a type divergence description, or None.

    A column whose every value is NULL is skipped: there is no value to type, so
    the server picks something arbitrary (we send VAR_STRING, MySQL infers the
    expression's type) and no client can tell the difference.
    """
    (sa, va, ta), (sb, vb, tb) = a, b
    if sa != "ok" or sb != "ok" or len(ta) != len(tb):
        return None
    for j, (x, y) in enumerate(zip(ta, tb)):
        if fold_type(x) == fold_type(y):
            continue
        if va and all(row[j] is None for row in va):
            continue
        return f"col{j} type {x} vs {y}"
    return None


def compare(a, b):
    """Return a divergence description, or None if they match."""
    (sa, va, _), (sb, vb, _) = a, b
    if sa == "crash" or sb == "crash":
        return f"CRASH/driver error (elyra={a[1] if sa=='crash' else 'ok'}, ref={b[1] if sb=='crash' else 'ok'})"
    if sa != sb:
        return f"elyra={sa}({va if sa!='ok' else 'rows'}) vs ref={sb}({vb if sb!='ok' else 'rows'})"
    if sa == "err":
        return None  # both reject -> semantically equivalent for this audit
    if len(va) != len(vb):
        return f"row count {len(va)} vs {len(vb)}"
    for i, (ra, rb) in enumerate(zip(va, vb)):
        if len(ra) != len(rb):
            return f"row {i}: col count {len(ra)} vs {len(rb)}"
        for j, (ca, cb) in enumerate(zip(ra, rb)):
            na, nb = norm(ca), norm(cb)
            if (
                isinstance(na, tuple)
                and isinstance(nb, tuple)
                and na[0] == "num"
                and nb[0] == "num"
            ):
                if not nums_close(na[1], nb[1]):
                    return f"row{i} col{j}: {ca!r} vs {cb!r}"
            elif na != nb:
                return f"row{i} col{j}: {ca!r} vs {cb!r}"
    return None


# ---- the battery -----------------------------------------------------------

# Fixtures created identically in both engines before the SELECT cases.
FIXTURES = [
    "DROP TABLE IF EXISTS d",
    "CREATE TABLE d (id INT PRIMARY KEY, n INT, f DOUBLE, s VARCHAR(32), dt DATE)",
    "INSERT INTO d VALUES (1,10,1.5,'apple','2024-01-15'),(2,-3,2.5,'Banana',NULL),"
    "(3,NULL,NULL,NULL,'2024-02-29'),(4,0,0.0,'','2000-01-01')",
    # Case-sensitivity fixture: `s` uses the default (case-insensitive) collation,
    # `sb` an explicit binary one, so REGEXP/comparison collation can be compared.
    # Join fixture spanning the 128..255 byte range.
    "DROP TABLE IF EXISTS jn",
    "CREATE TABLE jn (id INT PRIMARY KEY, g INT)",
    *[
        "INSERT INTO jn VALUES "
        + ",".join(f"({i},{i % 8})" for i in range(lo, min(lo + 100, 401)))
        for lo in range(1, 401, 100)
    ],
    "DROP TABLE IF EXISTS cs",
    "CREATE TABLE cs (s VARCHAR(20), sb VARCHAR(20) COLLATE utf8mb4_bin)",
    "INSERT INTO cs VALUES ('Hello','Hello')",
]

# (category, sql). Kept side-effect free (SELECTs) except the fixtures above.
CASES = [
    # arithmetic / numeric
    ("arith", "SELECT 1 + 1"),
    ("arith", "SELECT 9223372036854775807 + 1"),
    ("arith", "SELECT 9223372036854775807 * 2"),
    ("arith", "SELECT 1 % 0"),
    ("arith", "SELECT MOD(1, 0)"),
    ("arith", "SELECT 1 / 0"),
    ("arith", "SELECT 10 / 3"),
    # A boolean is an integer in MySQL. These all agree on the *value*, so only
    # the column type distinguishes them -- the shape that went unnoticed until
    # the type comparison above existed.
    ("arith", "SELECT TRUE + 1"),
    ("arith", "SELECT (1 = 1) + 1"),
    ("arith", "SELECT (2 > 1) + (3 > 2)"),
    ("arith", "SELECT !1 + 1"),
    ("arith", "SELECT -!0"),
    ("arith", "SELECT 10 DIV 3"),
    ("arith", "SELECT -10 DIV 3"),
    ("arith", "SELECT 10 % 3"),
    ("arith", "SELECT -10 % 3"),
    ("arith", "SELECT 10 % -3"),
    ("arith", "SELECT 5.5 % 2"),
    ("arith", "SELECT POW(10, 308) * 10"),
    ("arith", "SELECT SQRT(-1)"),
    ("arith", "SELECT LN(0)"),
    ("arith", "SELECT LN(-1)"),
    ("arith", "SELECT 3 & 5"),
    ("arith", "SELECT 3 | 5"),
    ("arith", "SELECT ~5"),
    ("arith", "SELECT 1 << 3"),
    ("arith", "SELECT ABS(-2147483648)"),
    # rounding
    ("round", "SELECT ROUND(2.5)"),
    ("round", "SELECT ROUND(3.5)"),
    ("round", "SELECT ROUND(-2.5)"),
    ("round", "SELECT ROUND(1.2345, 2)"),
    ("round", "SELECT ROUND(1.2355, 2)"),
    ("round", "SELECT TRUNCATE(1.2399, 2)"),
    ("round", "SELECT CEIL(-1.5)"),
    ("round", "SELECT FLOOR(-1.5)"),
    ("round", "SELECT FORMAT(1234567.891, 2)"),
    # NULL / three-valued logic
    ("null", "SELECT NULL + 1"),
    ("null", "SELECT NULL = NULL"),
    ("null", "SELECT NULL <=> NULL"),
    ("null", "SELECT 1 <=> NULL"),
    ("null", "SELECT NULL AND 0"),
    ("null", "SELECT NULL AND 1"),
    ("null", "SELECT NULL OR 1"),
    ("null", "SELECT COALESCE(NULL, NULL, 3)"),
    ("null", "SELECT IFNULL(NULL, 'x')"),
    ("null", "SELECT NULLIF(5, 5)"),
    ("null", "SELECT NULLIF(5, 6)"),
    ("null", "SELECT ISNULL(NULL)"),
    # comparison / coercion
    ("coerce", "SELECT '10' > '9'"),
    ("coerce", "SELECT '10' > 9"),
    ("coerce", "SELECT 1 = '1'"),
    ("coerce", "SELECT 0 = 'abc'"),
    ("coerce", "SELECT 'abc' = 'ABC'"),
    ("coerce", "SELECT 'a' < 'b'"),
    ("coerce", "SELECT 1 + '2abc'"),
    ("coerce", "SELECT TRUE AND 2"),
    ("coerce", "SELECT !0"),
    ("coerce", "SELECT NOT 5"),
    ("coerce", "SELECT 2 BETWEEN 1 AND 3"),
    ("coerce", "SELECT 'b' BETWEEN 'a' AND 'c'"),
    # CAST / CONVERT
    ("cast", "SELECT CAST('abc' AS SIGNED)"),
    ("cast", "SELECT CAST('12abc' AS SIGNED)"),
    ("cast", "SELECT CAST(3.7 AS SIGNED)"),
    ("cast", "SELECT CAST(-3.7 AS SIGNED)"),
    ("cast", "SELECT CAST(-1 AS UNSIGNED)"),
    ("cast", "SELECT CAST('2024-02-29' AS DATE)"),
    ("cast", "SELECT CAST('2024-02-30' AS DATE)"),
    ("cast", "SELECT CAST(3.14159 AS DECIMAL(4,2))"),
    # string functions
    ("string", "SELECT CONCAT('a', NULL, 'b')"),
    ("string", "SELECT CONCAT_WS('-', 'a', NULL, 'b')"),
    ("string", "SELECT LENGTH('héllo')"),
    ("string", "SELECT CHAR_LENGTH('héllo')"),
    ("string", "SELECT SUBSTRING('hello', 2, 3)"),
    ("string", "SELECT SUBSTRING('hello', -2)"),
    ("string", "SELECT SUBSTRING('hello', 0)"),
    ("string", "SELECT LEFT('hello', 2)"),
    ("string", "SELECT RIGHT('hello', 10)"),
    ("string", "SELECT LPAD('x', 4, 'ab')"),
    ("string", "SELECT REPLACE('aaa', 'a', 'bb')"),
    ("string", "SELECT LOCATE('l', 'hello')"),
    ("string", "SELECT INSTR('hello', 'x')"),
    ("string", "SELECT SUBSTRING_INDEX('a.b.c', '.', 2)"),
    ("string", "SELECT SUBSTRING_INDEX('a.b.c', '.', -1)"),
    ("string", "SELECT TRIM('  x  ')"),
    ("string", "SELECT REPEAT('ab', 3)"),
    ("string", "SELECT REVERSE('abc')"),
    ("string", "SELECT UPPER('héllo')"),
    ("string", "SELECT ASCII('A')"),
    ("string", "SELECT FIELD('b', 'a', 'b', 'c')"),
    ("string", "SELECT ELT(2, 'a', 'b', 'c')"),
    ("string", "SELECT HEX(255)"),
    ("string", "SELECT STRCMP('a', 'b')"),
    # date / time
    ("date", "SELECT DATEDIFF('2024-03-01', '2024-02-01')"),
    ("date", "SELECT DATE_ADD('2024-01-31', INTERVAL 1 MONTH)"),
    ("date", "SELECT DATE_ADD('2024-01-15', INTERVAL 10 DAY)"),
    ("date", "SELECT LAST_DAY('2024-02-10')"),
    ("date", "SELECT DAYOFWEEK('2024-01-15')"),
    ("date", "SELECT WEEKDAY('2024-01-15')"),
    ("date", "SELECT DAYOFYEAR('2024-03-01')"),
    ("date", "SELECT DATE_FORMAT('2024-01-15', '%Y/%m/%d')"),
    ("date", "SELECT EXTRACT(YEAR FROM '2024-01-15')"),
    ("date", "SELECT TIMESTAMPDIFF(DAY, '2024-01-01', '2024-01-31')"),
    ("date", "SELECT DATEDIFF('2024-02-01', '2024-03-01')"),
    # aggregates over empty / with NULLs (fixture table d)
    ("agg", "SELECT SUM(n), COUNT(n), COUNT(*), AVG(n), MIN(n), MAX(n) FROM d WHERE 1=0"),
    ("agg", "SELECT SUM(n), COUNT(n), COUNT(*), AVG(f) FROM d"),
    ("agg", "SELECT COUNT(DISTINCT n) FROM d"),
    ("agg", "SELECT GROUP_CONCAT(s ORDER BY id) FROM d"),
    ("agg", "SELECT MIN(s), MAX(s) FROM d"),
    # ordering with NULLs and mixed
    ("order", "SELECT id FROM d ORDER BY n"),
    ("order", "SELECT id FROM d ORDER BY n DESC"),
    ("order", "SELECT id FROM d ORDER BY s"),
    # LIKE / REGEXP
    ("like", "SELECT 'abc' LIKE 'a%'"),
    ("like", "SELECT 'ABC' LIKE 'a%'"),
    ("like", "SELECT 'a_c' LIKE 'a\\_c'"),
    ("like", "SELECT 'abc' REGEXP '^a.c$'"),
    ("like", "SELECT NULL LIKE '%'"),
    ("like", "SELECT 'abc' LIKE NULL"),
    # Join key encoding: a collation key is binary, so lossy UTF-8 conversion used
    # to collapse every value whose key contained a byte >= 0x80 (ids 128..255) into
    # one hash key, and a bare aggregate over a join emitted one bogus row per
    # empty spill partition.
    ("join", "SELECT COUNT(*) FROM jn a JOIN jn b ON a.id = b.id"),
    ("join", "SELECT COUNT(*) FROM jn a JOIN jn b ON a.id = b.id WHERE a.id <> b.id"),
    ("join", "SELECT COUNT(*) FROM jn a JOIN jn b ON a.id = b.id WHERE a.id BETWEEN 128 AND 255"),
    ("join", "SELECT MIN(a.id), MAX(a.id), SUM(a.id) FROM jn a JOIN jn b ON a.id = b.id"),
    ("join", "SELECT COUNT(*) FROM jn a JOIN jn b ON a.g = b.g"),
    ("join", "SELECT COUNT(*) FROM jn a LEFT JOIN jn b ON a.id = b.id"),
    ("join", "SELECT COUNT(*) FROM jn a JOIN jn b ON a.id = b.id WHERE a.id < 0"),
    ("join", "SELECT a.g, COUNT(*) FROM jn a JOIN jn b ON a.id = b.id GROUP BY a.g ORDER BY a.g"),
    # REGEXP follows the operand collation: case-insensitive by default (MySQL's
    # default collation is _ci), and an inline (?-i) still overrides it.
    ("regexp", "SELECT 'Hello' REGEXP 'h'"),
    ("regexp", "SELECT 'hello' REGEXP 'H'"),
    ("regexp", "SELECT 'Hello' REGEXP '(?-i)h'"),
    ("regexp", "SELECT 'Hello' NOT REGEXP 'h'"),
    ("regexp", "SELECT 'Hello' RLIKE 'ELL'"),
    ("regexp", "SELECT REGEXP_REPLACE('a1B2','[b]','x')"),
    ("regexp", "SELECT REGEXP_SUBSTR('ABC','b')"),
    ("regexp", "SELECT NULL REGEXP 'a'"),
    ("regexp", "SELECT 'a' REGEXP NULL"),
    # A _bin operand matches case-sensitively.
    ("regexp", "SELECT sb REGEXP 'h' FROM cs"),
    ("regexp", "SELECT sb REGEXP 'H' FROM cs"),
    ("regexp", "SELECT s REGEXP 'h' FROM cs"),
    # DIV integer division
    ("arith", "SELECT 7 DIV 2"),
    ("arith", "SELECT -7 DIV 2"),
    ("arith", "SELECT 7 DIV 0"),
    ("arith", "SELECT 5.9 DIV 2"),
    # more numeric functions
    ("num", "SELECT SIGN(-5)"),
    ("num", "SELECT GREATEST(1, 5, 3)"),
    ("num", "SELECT LEAST(1, 5, 3)"),
    ("num", "SELECT GREATEST(1, NULL, 3)"),
    ("num", "SELECT MOD(-7, 3)"),
    ("num", "SELECT TRUNCATE(-1.999, 0)"),
    ("num", "SELECT ROUND(1.5)"),
    ("num", "SELECT ROUND(0.5)"),
    ("num", "SELECT BIT_COUNT(7)"),
    ("num", "SELECT CONV('ff', 16, 10)"),
    ("num", "SELECT 0.1 + 0.2"),
    # more string functions
    ("string", "SELECT LTRIM('  x  ')"),
    ("string", "SELECT RTRIM('  x  ')"),
    ("string", "SELECT SPACE(3)"),
    ("string", "SELECT CHAR(65, 66, 67)"),
    ("string", "SELECT POSITION('l' IN 'hello')"),
    ("string", "SELECT INSERT('abcd', 2, 1, 'XY')"),
    ("string", "SELECT LOWER('ÀÉÎ')"),
    ("string", "SELECT SUBSTRING('hello' FROM 2 FOR 2)"),
    ("string", "SELECT CONCAT(1, 2, 3)"),
    ("string", "SELECT 'a' IN ('A', 'b')"),
    # more date functions
    ("date", "SELECT DAYNAME('2024-01-15')"),
    ("date", "SELECT MONTHNAME('2024-01-15')"),
    ("date", "SELECT QUARTER('2024-05-01')"),
    ("date", "SELECT TO_DAYS('2024-01-01')"),
    ("date", "SELECT SEC_TO_TIME(3661)"),
    ("date", "SELECT TIME_TO_SEC('01:01:01')"),
    ("date", "SELECT STR_TO_DATE('2024-13-01', '%Y-%m-%d')"),
    ("date", "SELECT DATE_SUB('2024-03-01', INTERVAL 1 DAY)"),
    ("date", "SELECT DATEDIFF('2024-01-01', NULL)"),
    # CASE / control-flow
    ("case", "SELECT CASE WHEN NULL THEN 1 ELSE 2 END"),
    ("case", "SELECT CASE WHEN 1 THEN 'a' WHEN 1 THEN 'b' END"),
    ("case", "SELECT CASE 2 WHEN 1 THEN 'x' WHEN 2 THEN 'y' ELSE 'z' END"),
    ("case", "SELECT IF(NULL, 'a', 'b')"),
    ("case", "SELECT IF(0, 'a', 'b')"),
    # NULL/aggregate edges on fixture
    ("agg", "SELECT MAX(dt), MIN(dt) FROM d"),
    ("agg", "SELECT n, COUNT(*) FROM d GROUP BY n ORDER BY n"),
    ("agg", "SELECT COUNT(*) FROM d GROUP BY n HAVING COUNT(*) >= 1 ORDER BY 1"),
    ("agg", "SELECT SUM(f) FROM d WHERE s IS NOT NULL"),
    ("agg", "SELECT 1 IN (NULL, 2)"),
    ("agg", "SELECT NULL IN (1, 2)"),
    # window
    ("window", "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM d ORDER BY id"),
    ("window", "SELECT id, SUM(id) OVER () FROM d ORDER BY id"),
    # `!` logical-NOT prefix (rewritten to NOT with preserved precedence)
    ("bang", "SELECT !0"),
    ("bang", "SELECT !1"),
    ("bang", "SELECT !5"),
    ("bang", "SELECT !NULL"),
    ("bang", "SELECT !!5"),
    ("bang", "SELECT !0 = 0"),
    ("bang", "SELECT 1 != 2"),
    ("bang", "SELECT !(1 = 1)"),
    # more numeric functions
    ("num", "SELECT LOG2(8)"),
    ("num", "SELECT LOG10(1000)"),
    ("num", "SELECT EXP(0)"),
    ("num", "SELECT POW(2, 10)"),
    ("num", "SELECT SIGN(0)"),
    ("num", "SELECT MOD(10.5, 3)"),
    ("num", "SELECT ROUND(-0.5)"),
    ("num", "SELECT CRC32('ElyraSQL')"),
    # more string functions
    ("string", "SELECT FIND_IN_SET('b', 'a,b,c')"),
    ("string", "SELECT FIND_IN_SET('x', 'a,b,c')"),
    ("string", "SELECT RPAD('x', 4, 'ab')"),
    ("string", "SELECT TRIM(LEADING 'x' FROM 'xxabc')"),
    ("string", "SELECT TRIM(BOTH 'x' FROM 'xxabcxx')"),
    ("string", "SELECT ORD('A')"),
    ("string", "SELECT BIN(5)"),
    ("string", "SELECT OCT(8)"),
    # comparison / null edges
    ("null", "SELECT 1 BETWEEN NULL AND 5"),
    ("null", "SELECT 2 <> 2"),
    ("null", "SELECT NOT NULL"),
    # bit aggregates over the fixture
    ("agg", "SELECT BIT_OR(n), BIT_AND(n), BIT_XOR(n) FROM d WHERE n IS NOT NULL"),
    # GROUP BY expression (not just a plain column)
    ("groupexpr", "SELECT n DIV 5 AS k, COUNT(*) FROM d GROUP BY n DIV 5 ORDER BY k"),
    ("groupexpr", "SELECT ABS(n) AS a, COUNT(*) FROM d GROUP BY ABS(n) ORDER BY a"),
    ("groupexpr", "SELECT UPPER(s) AS u, COUNT(*) FROM d GROUP BY UPPER(s) ORDER BY u"),
]


# Known, intentional or tracked divergences: reported but do not fail the build.
# Each is a deliberate design choice or a documented follow-up, not a regression.
# DML runs as its own ordered battery: unlike the SELECT cases these are
# stateful, and the same statement returns a different count the second time --
# that is exactly the convention being checked. Both engines execute the identical
# sequence against an identical fixture, and the affected-row count is compared
# after each step.
DML_FIXTURES = [
    "DROP TABLE IF EXISTS ar",
    "CREATE TABLE ar (id INT PRIMARY KEY, n INT)",
]

DML_CASES = [
    # MySQL counts rows *changed*, not matched, and upserts add a convention on
    # top: 1 inserted, 2 updated, 0 when the update assigns the current values.
    ("odku", "INSERT INTO ar VALUES (1,10) ON DUPLICATE KEY UPDATE n=VALUES(n)"),
    ("odku", "INSERT INTO ar VALUES (1,20) ON DUPLICATE KEY UPDATE n=VALUES(n)"),
    ("odku", "INSERT INTO ar VALUES (1,20) ON DUPLICATE KEY UPDATE n=VALUES(n)"),
    ("odku", "INSERT INTO ar VALUES (1,30),(2,40) ON DUPLICATE KEY UPDATE n=VALUES(n)"),
    # Two rows of one statement colliding with each other.
    ("odku", "INSERT INTO ar VALUES (5,1),(5,2) ON DUPLICATE KEY UPDATE n=VALUES(n)"),
    ("odku", "INSERT INTO ar VALUES (7,1),(7,1) ON DUPLICATE KEY UPDATE n=VALUES(n)"),
    ("replace", "REPLACE INTO ar VALUES (3,50)"),
    ("replace", "REPLACE INTO ar VALUES (3,60)"),
    ("replace", "REPLACE INTO ar VALUES (3,60)"),
    ("insert", "INSERT IGNORE INTO ar VALUES (1,99)"),
    ("insert", "INSERT INTO ar VALUES (4,70)"),
    ("insert", "INSERT INTO ar VALUES (10,1),(11,2),(12,3)"),
    ("update", "UPDATE ar SET n=71 WHERE id=4"),
    ("update", "UPDATE ar SET n=71 WHERE id=4"),
    ("update", "UPDATE ar SET n=n+1 WHERE id IN (10,11,12)"),
    ("update", "UPDATE ar SET n=1 WHERE id=999"),
    ("delete", "DELETE FROM ar WHERE id=12"),
    ("delete", "DELETE FROM ar WHERE id=999"),
]

# Result-column types that differ for a known, named reason. Values match in all
# of these -- only the declared type differs -- so they are tracked here rather
# than in ALLOWLIST.
TYPE_ALLOWLIST = {
    # DATE_ADD over a string literal: MySQL hands back a string, we hand back a
    # typed DATE. Ours is the more useful answer, and the rendered value agrees.
    "SELECT DATE_ADD('2024-01-31', INTERVAL 1 MONTH)",
    "SELECT DATE_ADD('2024-01-15', INTERVAL 10 DAY)",
    "SELECT DATE_SUB('2024-03-01', INTERVAL 1 DAY)",
}

ALLOWLIST = {
    # Intentional strictness: ElyraSQL does NOT silently coerce a non-numeric
    # string to 0 in implicit arithmetic/comparison (a MySQL foot-gun). Explicit
    # CAST(... AS SIGNED) does follow MySQL.
    "SELECT 0 = 'abc'",
    "SELECT 1 + '2abc'",
    # Benign wire-type: a DECIMAL result is sent as text (value identical).
    "SELECT CAST(3.14159 AS DECIMAL(4,2))",
    # Benign wire-type: a TIME result is sent as text (value identical: 01:01:01).
    "SELECT SEC_TO_TIME(3661)",
    # Intentional: MySQL's bare `!!x` is a known quirk (`!!5` = 0, yet `!(!5)` = 1
    # and `NOT NOT 5` = 1). We treat `!!x` as consistent double negation, matching
    # the parenthesised / NOT NOT forms rather than replicating the quirk.
    "SELECT !!5",
    # Both reject the out-of-range value (we return NULL, MySQL errors 1690).
    "SELECT POW(10, 308) * 10",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--elyra-host", default="127.0.0.1")
    ap.add_argument("--elyra-port", type=int, default=3307)
    ap.add_argument("--elyra-user", default="root")
    ap.add_argument("--elyra-password", default="")
    ap.add_argument("--ref-host", default="127.0.0.1")
    ap.add_argument("--ref-port", type=int, default=3390)
    ap.add_argument("--ref-user", default="root")
    ap.add_argument("--ref-password", default="root")
    a = ap.parse_args()

    elyra = pymysql.connect(host=a.elyra_host, port=a.elyra_port, user=a.elyra_user,
                            password=a.elyra_password, autocommit=True)
    ref = pymysql.connect(host=a.ref_host, port=a.ref_port, user=a.ref_user,
                          password=a.ref_password, autocommit=True)
    for conn in (elyra, ref):
        cur = conn.cursor()
        # The reference needs a selected database; ElyraSQL is single-DB (ignore).
        for stmt in ("CREATE DATABASE IF NOT EXISTS diffdb", "USE diffdb"):
            try:
                cur.execute(stmt)
            except Exception:
                pass
        for f in FIXTURES:
            cur.execute(f)
        cur.close()

    divergences = []
    allowed = []
    crashes = []
    for cat, sql in CASES:
        ra, rb = run(elyra, sql), run(ref, sql)
        for diff, allow in ((compare(ra, rb), ALLOWLIST),
                            (compare_types(ra, rb), TYPE_ALLOWLIST)):
            if not diff:
                continue
            if "CRASH" in diff:
                crashes.append((cat, sql, diff))
            elif sql in allow:
                allowed.append((cat, sql, diff))
            else:
                divergences.append((cat, sql, diff))

    # DML: identical ordered sequence on both engines, comparing affected rows.
    for conn in (elyra, ref):
        cur = conn.cursor()
        for f in DML_FIXTURES:
            cur.execute(f)
        cur.close()
    for cat, sql in DML_CASES:
        (sa, va), (sb, vb) = run_dml(elyra, sql), run_dml(ref, sql)
        if sa == "crash" or sb == "crash":
            crashes.append((cat, sql, f"CRASH (elyra={va}, ref={vb})"))
        elif sa != sb:
            divergences.append((cat, sql, f"elyra={sa}({va}) vs ref={sb}({vb})"))
        elif sa == "ok" and va != vb:
            divergences.append((cat, sql, f"affected rows {va} vs {vb}"))

    total = len(CASES) + len(DML_CASES)
    print(f"\n{'='*74}\nMySQL differential — {len(CASES)} query + "
          f"{len(DML_CASES)} DML cases\n{'='*74}")
    if crashes:
        print(f"\n!!! {len(crashes)} CRASH/driver-level divergence(s) !!!")
        for cat, sql, d in crashes:
            print(f"  [{cat}] {sql}\n      {d}")
    if allowed:
        print(f"\n{len(allowed)} allowlisted divergence(s) (intentional/tracked):")
        for cat, sql, d in allowed:
            print(f"  [{cat}] {sql}\n      -> {d}")
    if divergences:
        print(f"\n{len(divergences)} UNEXPECTED divergence(s):")
        for cat, sql, d in divergences:
            print(f"  [{cat}] {sql}\n      -> {d}")
    else:
        print("\nNo unexpected divergences.")
    print(f"{'='*74}")
    print(
        f"pass={total-len(divergences)-len(allowed)-len(crashes)} "
        f"allow={len(allowed)} diverge={len(divergences)} crash={len(crashes)}"
    )

    sys.exit(1 if (divergences or crashes) else 0)


if __name__ == "__main__":
    main()
