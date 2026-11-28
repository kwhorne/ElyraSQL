#!/usr/bin/env python3
"""PyMySQL compatibility smoke test for ElyraSQL.

PyMySQL is a pure-Python MySQL driver that binds parameters client-side, so this
exercises a different client stack than the Rust/PHP harnesses. Connection is
read from the environment (ELYRASQL_HOST/PORT/USER/PASS), defaulting to
127.0.0.1:3307 root/no-password. Exits non-zero on failure.
"""
import os
import sys

import pymysql


def main() -> int:
    conn = pymysql.connect(
        host=os.environ.get("ELYRASQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("ELYRASQL_PORT", "3307")),
        user=os.environ.get("ELYRASQL_USER", "root"),
        password=os.environ.get("ELYRASQL_PASS", ""),
        autocommit=True,
    )
    cur = conn.cursor()
    failures = 0

    def check(name, cond, extra=""):
        nonlocal failures
        if cond:
            print(f"  ok   {name}")
        else:
            failures += 1
            print(f"  FAIL {name}  {extra}")

    cur.execute("SELECT VERSION()")
    ver = cur.fetchone()[0]
    check("version reports ElyraSQL", "ElyraSQL" in ver, ver)

    cur.execute("DROP TABLE IF EXISTS py_items")
    cur.execute(
        "CREATE TABLE py_items (id INT PRIMARY KEY, name VARCHAR(32), price DECIMAL(8,2))"
    )
    # parameterised insert (PyMySQL substitutes client-side)
    cur.executemany(
        "INSERT INTO py_items (id, name, price) VALUES (%s, %s, %s)",
        [(1, "widget", 9.99), (2, "gadget", 19.50), (3, "gizmo", 4.25)],
    )
    check("insert last_insert_id / rowcount", cur.rowcount == 3, str(cur.rowcount))

    cur.execute("SELECT COUNT(*) FROM py_items")
    check("count", cur.fetchone()[0] == 3)

    cur.execute("SELECT name FROM py_items WHERE id = %s", (2,))
    check("parameterised select", cur.fetchone()[0] == "gadget")

    cur.execute("SELECT SUM(price) FROM py_items")
    total = float(cur.fetchone()[0])
    check("decimal sum", abs(total - 33.74) < 0.001, str(total))

    cur.execute("UPDATE py_items SET price = price * 2 WHERE id = 1")
    cur.execute("SELECT price FROM py_items WHERE id = 1")
    check("update", float(cur.fetchone()[0]) == 19.98)

    cur.execute("DELETE FROM py_items WHERE id = 3")
    cur.execute("SELECT COUNT(*) FROM py_items")
    check("delete", cur.fetchone()[0] == 2)

    # An UPDATE that leaves its row as it was: 0 rows by default, 1 for a
    # client with CLIENT_FOUND_ROWS. Django connects with that flag and takes
    # 0 for a missing row, so its save() of an unchanged row INSERTed it.
    n = cur.execute("UPDATE py_items SET price = price WHERE id = 1")
    check("unchanged update counts 0 rows", n == 0, str(n))
    found = pymysql.connect(
        host=os.environ.get("ELYRASQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("ELYRASQL_PORT", "3307")),
        user=os.environ.get("ELYRASQL_USER", "root"),
        password=os.environ.get("ELYRASQL_PASS", ""),
        autocommit=True,
        client_flag=pymysql.constants.CLIENT.FOUND_ROWS,
    )
    fcur = found.cursor()
    n = fcur.execute("UPDATE py_items SET price = price WHERE id = 1")
    check("unchanged update counts 1 row with FOUND_ROWS", n == 1, str(n))
    fcur.close()
    found.close()

    cur.close()
    conn.close()

    if failures:
        print(f"\n{failures} check(s) failed", file=sys.stderr)
        return 1
    print("\nall PyMySQL checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
