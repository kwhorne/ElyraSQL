# Installation

ElyraSQL release builds target **Ubuntu 24.04+** and **Apple Silicon macOS
11+**. Intel Macs are not supported.

!!! warning "Upgrading to 1.5.0 or later"

    1.5.0 changed the default collation to `utf8mb4_0900_ai_ci`, which changes the
    bytes under which text is stored. A database created by an earlier version is
    migrated automatically the first time 1.5.0 or later opens it: text index entries are
    rebuilt and text primary keys re-keyed, before any connection is accepted.
    Databases whose indexed text is pure ASCII are not rewritten.

    The migration writes in batches so that a large table cannot exhaust memory at
    startup, and it is **idempotent**: an already re-keyed row encodes to the key it
    is already stored under. The version marker is written only once every table is
    done, so an interrupted upgrade simply resumes on the next start. **Take a backup
    first, and note that downgrading to 1.4.x afterwards is not supported.**

!!! info "Upgrading to 1.12.15 — MySQL index definitions"

    No on-disk format change; a 1.12.14 database opens in 1.12.15 unchanged.

    - **Navicat and mysqldump dumps restore.** Index definitions as MySQL
      writes them -- `KEY k (a) USING BTREE`, `ASC` / `DESC` key parts,
      `COMMENT` / `VISIBLE` / `KEY_BLOCK_SIZE` options, `ALGORITHM` / `LOCK` --
      were refused with 1064, and with them every table that had one.
    - **A foreign key shares an index that leads with its columns**, as in
      MySQL. Tables created from now on get no extra `fk_<table>_N` index
      where a composite index already serves the key; existing tables keep
      theirs.

    If you are upgrading from 1.12.13 or earlier, read the 1.12.14 note below
    first: that release fixes a query that could stop the server.

!!! danger "Upgrade to 1.12.14 promptly — a query could stop the server"

    No on-disk format change; a 1.12.13 database opens in 1.12.14 unchanged.

    - **Any client could end the server** with a query such as `SELECT 1 DIV 2
      << 1`: the SQL parser panicked on what followed `DIV`, and the server
      aborts on a panic. A read-only account was enough. It is now a syntax
      error.
    - **Results that were wrong now follow MySQL**, so some queries return
      different rows or values than before:
        - `DIV` binds like `*` and `/`: `7 DIV 2 + 1` is 4 (was 2), and `a DIV b
          = 3` compares the quotient (it used to divide by the comparison).
        - A BIGINT compared above 9007199254740992 is exact: `COUNT(*) WHERE
          cust = 9007199254740993` no longer counts its neighbours, and an
          integer column against a string or float constant
          (`cust = '9007199254740992'`) or a DECIMAL compares exactly.
    - **`ALTER TABLE` refuses changes the existing rows break** (strict mode),
      with MySQL's error: `MODIFY ... NOT NULL` over NULLs (1138), a narrower
      integer type over larger values (1264), an `ENUM` over other values
      (1265), `ADD UNIQUE` / `CREATE UNIQUE INDEX` over duplicates (1062),
      `ADD FOREIGN KEY` over orphans (1452). These used to succeed and leave
      the rows in place: **if you ran such ALTERs, check those columns** —
      for duplicates under a unique index, and for values outside an `ENUM`
      or a column's NOT NULL.
    - Also new: `ADD CHECK`, the `MOD` operator, logical `XOR`, and `INSERT
      INTO t () VALUES ()`; an omitted NOT NULL column without a default is
      1364.

!!! warning "Upgrading to 1.12.13 — writes that MySQL refuses are refused"

    No on-disk format change; a 1.12.12 database opens in 1.12.13 unchanged.
    Three fixes **refuse statements 1.12.12 accepted**, as MySQL does:

    - **A multi-table `UPDATE`** (`UPDATE a JOIN b ... SET`, `UPDATE a, b SET`)
      now enforces unique keys, foreign keys, `CHECK`, `NOT NULL` and `ON
      UPDATE CASCADE`, and fires triggers. It used to write past all of them:
      **if you ran multi-table updates against unique keys, check those keys
      for duplicates** (`SELECT k, COUNT(*) FROM t GROUP BY k HAVING
      COUNT(*) > 1`) before upgrading.
    - **A scalar subquery that returns more than one row** is 1242. It used the
      first row instead, so `SELECT`s returned, and `UPDATE`s and `DELETE`s
      changed, rows MySQL refuses to choose.
    - **A write that reads its own table in a subquery** (`DELETE FROM t WHERE
      x > (SELECT AVG(x) FROM t)`) is 1093, or 1443 through a view. Read it
      through a derived table (`FROM (SELECT ...) AS d`) instead.

    Also new: `UPDATE IGNORE` works (it failed with 1146), subqueries work in
    `UPDATE ... SET` and in `JOIN ... ON`, and a join in parentheses works on
    the right of another join. `DROP TABLE` and `RENAME TABLE` take the
    table's triggers with them. A join whose `ON` adds a condition to its key
    (`... AND b.deleted_at IS NULL`) is no longer quadratic: 98 s became 0.07 s
    on two 20,000-row tables. Some errors now carry MySQL's code instead of
    1105 or 1452: 1048, 3819, 1265, 1451, 1066.

!!! warning "Upgrading to 1.12.12 — collation"

    No on-disk format change; a 1.12.11 database opens in 1.12.12 unchanged.

    - **`COLLATE` and the `BINARY` operator work in expressions**
      (`WHERE BINARY s = 'x'`, `ORDER BY s COLLATE utf8mb4_bin`), where they
      were refused.
    - **Results that were wrong now follow MySQL**, so a query may return
      different rows than before: on a `utf8mb4_bin` column, `LIKE` and `IN`
      are case-sensitive and `COUNT(DISTINCT)` counts `a` and `A` apart; on the
      default collation, `LIKE` folds accents (`'café' LIKE 'cafe'`); and
      `CAST(s AS BINARY) = 'x'` compares bytes, where it matched nothing.
    - **`MIN`/`MAX` of a text expression** (`MAX(UPPER(s))`) no longer ends
      the connection.

!!! warning "Upgrading to 1.12.11 — LOCK TABLES, cluster commit"

    No on-disk format change; a 1.12.10 database opens in 1.12.11 unchanged.

    - **`LOCK TABLES` locks what MySQL locks.** Another session's `SELECT` of
      a `WRITE`-locked table, a joined `UPDATE`/`DELETE`, the `SELECT` of an
      `INSERT ... SELECT`, a view over the table and `CREATE TRIGGER` now wait
      for the lock, where they went straight through. The session holding
      `LOCK TABLES` may now use only the tables it locked (`1100`) and write
      only those it locked `WRITE` (`1099`); code that locks one table and then
      reads another must lock both, as MySQL requires. A second `LOCK TABLES`
      replaces the first.
    - **`UPDATE t JOIN t2 ... SET t.c = <value>`** without `WHERE` needs
      `SELECT` on `t2`, as in MySQL.
    - **A cluster commits as soon as a majority has a write.** One unreachable
      follower no longer adds 500 ms to every write.

!!! warning "Upgrading to 1.12.10 — roles, INSERT IGNORE and REPLACE"

    No on-disk format change; a 1.12.9 database opens in 1.12.10 unchanged.

    - **`DROP ROLE` takes away what the role granted** (security fix). The
      role's table and column grants and its members' memberships were left
      behind, so its members kept every privilege it had given them, while
      `SHOW GRANTS` showed only `USAGE`. A role dropped before 1.12.10 no
      longer grants anything once you upgrade; nothing needs to be run.
    - **`INSERT IGNORE` and `REPLACE` honour unique keys.** Both checked only
      the primary key, so a `UNIQUE` column could be given a value it already
      held. `INSERT IGNORE` now skips such a row and `REPLACE` deletes the row
      it collides with. A table written this way before may hold duplicates;
      `SELECT col, COUNT(*) FROM t GROUP BY col HAVING COUNT(*) > 1` finds them.
    - **`REPLACE` applies foreign keys** to the rows it deletes, as `DELETE`
      does: a `RESTRICT` child now refuses it, and `ON DELETE CASCADE` /
      `SET NULL` act on the children -- also when the replacement has the same
      primary key, or is identical. Code that `REPLACE`s a referenced parent
      row will see this.

!!! info "Upgrading to 1.12.9 — mysqldump, views in the catalog"

    No on-disk format change; a 1.12.8 database opens in 1.12.9 unchanged.

    - **mysqldump can dump an ElyraSQL database** with its rows, views,
      triggers, procedures, partitioning and statistics, and the dump restores
      into ElyraSQL or MySQL. Name the database: `--all-databases` still gives
      an empty dump. See [Backup](backup.md#dumping-with-mysqldump).
    - **A procedure records how it was written**, so `SHOW CREATE PROCEDURE`
      and a dump give its parameter types back. One created before 1.12.9 is
      given back without them until it is created again.
    - **`BLOB` values from a MySQL dump restore.** A `_binary '...'` literal
      with bytes that are not UTF-8 ended the connection.
    - **A partition bound followed by `ENGINE = InnoDB`, as in every MySQL
      dump, is kept.** It became `MAXVALUE`. A table restored from such a dump
      before 1.12.9 has that partitioning; `SHOW CREATE TABLE` now shows it.
    - **Views are listed in `information_schema.TABLES`, `SHOW TABLE STATUS`
      and `information_schema.COLUMNS`**, and `DESCRIBE <view>` works. A tool
      that reads the catalog will now see them.

!!! info "Upgrading to 1.12.8"

    No on-disk format change; a 1.12.7 database opens in 1.12.8 unchanged.

    - **`ORDER BY` a joined table's column that is not selected sorts by that
      column.** `SELECT x.name, y.id ... ORDER BY x.id, y.id` sorted by `y.id`
      instead of `x.id`, so the rows came back in the wrong order and a
      `LIMIT` kept the wrong ones. A query of that shape now returns MySQL's
      order; one that relied on the old order will see a different one.

!!! warning "Upgrading to 1.12.7 — who is asking, views, and mysqldump"

    No on-disk format change; a 1.12.6 database opens in 1.12.7 unchanged, and
    its views read exactly as before. Several answers become MySQL's, and code
    that depended on the old ones will notice:

    - **`USER()`, `CURRENT_USER()` and `CONNECTION_ID()` answer for the
      connection.** They were `root@%` and `1` for everyone. A comparison with
      `'root@%'`, or a column default or trigger that recorded it, now sees the
      real account.
    - **Views run as their definer.** A view created from now on records its
      definer (the `DEFINER` clause, or whoever creates it). When that is a
      stored account, its privileges on the tables are checked;
      `SQL SECURITY INVOKER` checks the querying user's instead. A refusal is
      error 1356. Views created before 1.12.7 run as they always have.
    - **Version comments run.** The body of `/*!NNNNN ... */` up to 8.0.12 is
      code, as in MySQL. A script that carried statements in them -- every
      mysqldump file does -- now runs them, and one that fails now reports it.
    - **Errors MySQL gives, with MySQL's codes:** `CREATE ROLE` of an existing
      account (it overwrote it), `DROP ROLE` or `DROP PROCEDURE` of a missing
      one (both succeeded), all 1396 or 1305; `CREATE USER` / `DROP USER` without
      `IF [NOT] EXISTS` now give 1396 instead of 1105, and the `IF` forms are
      honoured.

    A mysqldump of a MySQL 8.4 database now restores with the `mysql` client --
    tables, rows, views, triggers and procedures -- except stored functions,
    which ElyraSQL does not have yet. See
    [Importing a MySQL dump](backup.md#importing-a-mysql-dump).

!!! info "Upgrading to 1.12.6"

    No on-disk format change; a 1.12.5 database opens in 1.12.6 unchanged.

    - **The server stops on SIGTERM**, as `docker stop`, systemd and Kubernetes
      send it. In a container it used to ignore the signal until `docker stop`
      gave up and killed it (exit 137, after 10 s by default); elsewhere it
      ended at once, reported as a crash. It now flushes and exits with 0. A
      supervisor that counted those exits as failures will see clean stops,
      and anything that relied on SIGTERM being ignored will find the server
      stopping.

!!! info "Upgrading to 1.12.5"

    No on-disk format change; a 1.12.4 database opens in 1.12.5 unchanged, and
    nothing that ran before answers differently. Nodes talk to each other as
    before, so a cluster can be upgraded one node at a time.

    - **Cluster mode: check your followers.** A follower could keep a row the
      leader had deleted, when the insert and the delete reached it in one batch
      of log entries — typically while it caught up after a restart or an
      outage. 1.12.5 stops this from happening again but does not repair a
      follower that already differs. After upgrading, compare each follower's
      tables with the leader's (`SELECT COUNT(*)` per table is enough to find
      one).
    - **Column cache and zone maps.** With `ELYRASQL_COLUMN_CACHE_MB` or
      `ELYRASQL_ZONE_MAPS` on, an aggregate could be answered from a stale
      cache entry some time after a restart. Only answers were wrong, never
      stored data, so there is nothing to repair.
    - **TLS**: rustls 0.23.46 and rustls-webpki 0.103.16, for six security
      advisories. They cover the MySQL port's TLS and the cluster and
      replication links.

!!! warning "Upgrading to 1.12.4 — an aggregate subquery is refused sooner"

    No on-disk format change; a 1.12.3 database opens in 1.12.4 unchanged. One
    change can surface as a query that used to run and now does not:

    - **A correlated subquery inside an aggregate expression is refused while the
      statement is planned**, where it used to be refused by the row evaluator —
      which meant it was *accepted* whenever no row reached the expression. The
      same statement passed against an empty table and failed against a populated
      one, so a query of this shape could sit in a codebase looking healthy until
      it met real data. It is now refused either way, and the message names the
      aggregate it was found in rather than `WHERE`. Rewrite it by resolving the
      keys first and binding them as a list — one query instead of one per row.
      The **uncorrelated** form now works where it did not, so a lookup that does
      not reference the outer row needs no change.
    - Two things that were refused now answer: a subquery inside a `CASE` arm or
      a function argument — `SELECT ABS((SELECT -1))`,
      `SELECT CASE WHEN 1 IN (SELECT 1) THEN 1 ELSE 0 END`. Nothing that ran
      before stops running.

    This release also fixes unbounded memory growth in the page cache under a
    write workload. A long-running server is the case it affects, and a restart
    was the only remedy before.

!!! warning "Upgrading to 1.12.3 — implicit commit, as in MySQL"

    No on-disk format change; a 1.12.2 database opens in 1.12.3 unchanged. Three
    fixes change what existing code can see:

    - **DDL commits the open transaction first.** `CREATE`, `ALTER`, `DROP`,
      `RENAME`, `TRUNCATE`, account statements, `LOCK TABLES`, `ANALYZE TABLE`
      and a new `BEGIN` now commit an open transaction before they run, as MySQL
      does. A `ROLLBACK` after such a statement no longer undoes the writes made
      before it. Code that relied on DDL being part of the transaction must
      commit or roll back first. A second `BEGIN` no longer discards the open
      transaction's writes: it commits them. Temporary tables and `REFRESH
      MATERIALIZED VIEW` stay inside the transaction.
    - **`\_` and `\%` in `LIKE` match literally.** `'axb' LIKE 'a\_b'` is now
      false, as in MySQL; it was true. Searches that escape their input this way
      return fewer, correct, rows.
    - **`SHOW TABLES` applies `LIKE` and `WHERE`, and lists views.** `SHOW TABLES
      LIKE 'name'` used to list every table.

!!! info "Upgrading to 1.12.2"

    No on-disk format change; a 1.12.1 database opens in 1.12.2 unchanged.

    - **`CALL` returns the procedure's result sets.** Each `SELECT` a procedure
      runs now comes back as its own result set, followed by the `CALL`'s
      status, as in MySQL. A client that called a procedure and read only an
      OK will now find result sets first: read them, or move to the next
      result (`nextRowset()` in PDO, `nextset()` in PyMySQL) until the end.
    - **The server offers `CLIENT_MULTI_RESULTS`.** Every current driver
      supports it. Only a `CALL` uses it; every other statement still returns
      a single result.
    - **Inside procedures**, a local variable selected on its own names the
      column after itself (`SELECT n` gives a column `n`), as in MySQL.

!!! info "Upgrading to 1.12.1"

    No on-disk format change; a 1.12.0 database opens in 1.12.1 unchanged. Three
    fixes change what existing code can see:

    - **Read-only transactions refuse writes.** In `START TRANSACTION READ ONLY`,
      or with `SET SESSION TRANSACTION READ ONLY` / `@@transaction_read_only`, an
      `INSERT`, `UPDATE`, `DELETE`, `SELECT ... FOR UPDATE` or DDL now fails with
      error 1792, as in MySQL. They used to run, and only a `ROLLBACK` undid them.
      An unscoped `SET TRANSACTION` inside an open transaction is now refused
      (1568).
    - **`SELECT ... INTO @var` assigns the variable and returns no rows.** It used
      to be ignored and return the row, so code that read that row must read
      `@var` instead. Inside procedures, `SELECT ... INTO` a local works too.
    - **`DROP DATABASE other_name` reports 1008** (*database doesn't exist*) when
      the name is neither `elyra` nor this connection's own database name, rather
      than 1235 *not supported*. `IF EXISTS` is still a no-op.

!!! warning "Upgrading to 1.12.0 — security fixes and MySQL privileges"

    No on-disk format change; a 1.11.4 database opens in 1.12.0 unchanged.

    **Two security fixes.** Upgrade replicas as well as the primary.

    - **Column grants could be bypassed.** A user granted `SELECT(col)` on a
      table could read its other columns through a set operation (`... UNION
      SELECT ...`) or copy them into a table of their own with `INSERT ...
      SELECT`. Every table a statement reads is now checked; a column-restricted
      table is readable only through a plain single-table `SELECT`.
    - **Replicas missed schema and grant changes.** A replica did not enforce a
      column grant created on the primary after it had last checked for one,
      and kept serving a table's old definition after `ALTER TABLE`, until it
      was restarted.

    **Privileges now work as in MySQL.** This can change what existing setups
    see:

    - **A new account starts with no privileges** (`USAGE`). A script that runs
      `CREATE USER` and expects the account to read must grant it, e.g.
      `GRANT SELECT ON *.* TO 'app'` or `GRANT SELECT ON orders TO 'app'`.
      Accounts created before 1.12.0 keep the read access they had by default;
      `REVOKE SELECT ON *.* FROM 'app'` brings one in line. Accounts configured
      at startup (`--user`, `--auth`) keep their configured tier.
    - **Table and column grants grant read access on their own**, including
      after `REVOKE SELECT ON *.*`, which used to refuse them.
    - **An `UPDATE` or `DELETE` with a `WHERE` needs `SELECT` on its table**, as
      in MySQL. An account granted only `UPDATE` can no longer run `UPDATE t SET
      ... WHERE ...`; grant it `SELECT` as well.
    - **`--auth user:pass:write` accounts can write.** They could not `INSERT`,
      `UPDATE` or `DELETE` at all before.

    **Other changes a client can see:**

    - **`AVG` over an integer column or expression returns `DECIMAL`** with four
      decimals, as in MySQL, not `DOUBLE`. A client that decodes it strictly as a
      float (sqlx `f64`, for example) must accept a decimal. `MIN`/`MAX` of an
      integer expression is `BIGINT` rather than `DECIMAL`.
    - **The page cache defaults to a quarter of available memory** (the container
      limit, if lower), not a fixed 1 GiB, so scanning a large table can raise
      memory use on a shared host. `ELYRASQL_PAGE_CACHE_MB` caps it.
    - **`EXPLAIN` names only what runs.** `Aggregate: columnar group, zone maps`
      is now `Aggregate: columnar group`, followed by `, primary-key range`,
      `, zone maps` or `, columnar cache` when that is what will run. Match the
      prefix rather than the whole string.

!!! warning "Upgrading to 1.11.4 — security release"

    1.11.4 updates rustls to 0.23.45 for **RUSTSEC-2026-0285**. Its TLS 1.3
    handshake accepted handshake messages sent at the wrong encryption level when
    they followed a key-changing message in the same record, where RFC 8446
    requires the connection to be closed. The handshake transcript is still
    authenticated, so an attacker in the network path cannot alter or complete a
    handshake through it. The advisory rates it CVSS 5.3: low confidentiality
    impact, none on integrity or availability.

    Every release from 0.9.9 through 1.11.3 ships an affected rustls, on each path
    where ElyraSQL speaks TLS:

    - the MySQL listener, when started with `--tls-cert`/`--tls-key`;
    - replication and the cluster control plane (`ELYRASQL_CLUSTER_TLS_*`), as
      both server and client;
    - outbound HTTPS calls for AI embeddings.

    If you use none of these over TLS, nothing changes for you. Either way it is a
    drop-in upgrade: no on-disk format change and no behaviour change, so a 1.11.3
    database opens in 1.11.4 unchanged.

!!! info "Upgrading to 1.11.3"

    No on-disk format change; a 1.11.2 database opens in 1.11.3 unchanged. Four
    changes can alter what existing, working code sees:

    - **`SET @var` in a stored procedure now writes the session variable**, as in
      MySQL. It used to land in the procedure's local scope and vanish when `CALL`
      returned. A procedure that uses an `@name` its caller also uses now
      overwrites the caller's value; use a `DECLARE`d local for scratch values.
    - **`NOW()` and its family return one instant per statement.** A long
      `INSERT ... SELECT` that stamped rows with `NOW()` now stamps them all alike.
      `SYSDATE()` still reads the clock each time it runs.
    - **An embedded engine derives its expression-depth limit from the calling
      thread's stack.** `elyrasql` itself runs on 16 MiB worker stacks and keeps
      the configured ceiling (`ELYRASQL_MAX_EXPR_DEPTH`, default 2000). Under
      `elyra-embed` on a small thread, a deeply nested expression is now refused
      as *too deeply nested*, where before it either ran or overflowed the stack
      and aborted the process. Run the engine on a larger thread if you generate
      deep expressions.
    - **`EXPLAIN` fills `Extra` for grouped queries** with the aggregation path,
      such as `Aggregate: columnar scalar`. A tool that expected it empty will
      now see text.

!!! danger "Upgrading to 1.11.1 — binary data written through prepared statements"

    **If any application inserted BLOB or binary data using server-side prepared
    statements on 1.9.x through 1.11.0, that data is corrupted on disk.**

    A parameter that was not valid UTF-8 — an image, a hash, ciphertext,
    compressed content, anything a driver sends as bytes over the binary
    protocol — was rendered into SQL through a lossy text conversion. Every byte
    that was not valid UTF-8 was replaced with U+FFFD (`EF BF BD`), and no error
    was raised: seven bytes went in, fifteen came back. Drivers that use
    server-side prepared statements for binary columns include PDO without
    emulated prepares, Go `database/sql`, JDBC with `useServerPrepStmts`, and
    Python connectors with `prepared=True`. Text parameters and anything sent as
    plain SQL were never affected.

    1.11.1 fixes the path. It cannot repair the rows: the original bytes are
    gone. After upgrading:

    - **Identify BLOB/VARBINARY columns written through `?` parameters.** A
      corrupted value contains the three-byte sequence `EF BF BD` where the
      original had a byte that was not valid UTF-8;
      `SELECT COUNT(*) FROM t WHERE HEX(col) LIKE '%EFBFBD%'` finds candidates
      (a genuine U+FFFD in the source data is the only false positive).
    - **Re-insert them from the source** — the file store, the upstream system,
      a backup taken before the data was written through ElyraSQL.
    - Values that were valid UTF-8 as a whole, such as ASCII, are intact.

    No on-disk format change; a 1.11.0 database opens in 1.11.1 unchanged.

!!! danger "Upgrading to 1.9.9 or later — security release"

    **If you run replication, 1.9.8 and earlier expose your data. Upgrade.**

    Connecting to the replication port returned a full copy of the database:
    no credentials, no handshake, nothing sent by the peer. The guard only
    covered non-loopback binds, so a primary started with
    `--replication-listen 127.0.0.1:...` and no cluster secret was readable by
    any local process — or by anything that could get a TCP connection opened on
    its behalf.

    Two more, as serious: a replica never checked its primary's identity, so
    anything answering on the primary's address could feed it fabricated rows —
    **even with `ELYRASQL_CLUSTER_SECRET` set**, because authentication ran in
    one direction only. And `elyrasql replica` had no authentication flags at
    all, so its MySQL listener always accepted any username as `Admin` over
    replicated production data.

    **The fixes are breaking, deliberately.** After upgrading:

    - **A primary with `--replication-listen` needs `ELYRASQL_CLUSTER_SECRET`.**
      Without it the endpoint refuses to start and logs why; the server keeps
      serving, so watch for `replication endpoint stopped` in the log rather
      than assuming replication is running.
    - **A replica needs the same secret**, and refuses to start without it.
    - **Primary and replica must be upgraded together.** The handshake gained a
      step; a 1.9.9-or-later replica will not accept a 1.9.8 primary.
    - **A replica needs accounts**: `--user`/`--password`/`--auth USER:PASS:ROLE`,
      exactly as `serve` takes them.

    `ELYRASQL_ALLOW_OPEN_AUTH=1` opts out of all four, and is the honest way to
    say "this port is on a network I control". It is not a default.

    If you ran an exposed replication endpoint, rotate anything the data would
    have revealed. The port left no access log.

!!! warning "Upgrading to 1.9.8"

    Arithmetic that used to be approximate is now exact. Results and result
    *types* both change. None of it touches your data, and every change moves
    toward MySQL — but if you have code that compensated for the old behaviour,
    that compensation is now wrong.

    **Rounding a decimal gives a different answer at the halfway point.**

    ```sql
    SELECT ROUND(1.005, 2);   -- was 1.00, now 1.01 (MySQL: 1.01)
    ```

    1.005 has no exact binary form, so the old path rounded it down. Anything
    that reconciled against MySQL, a spreadsheet or an accounting system was
    seeing our answer, not theirs. **If you stored the old results, they do not
    match what the same query returns now.**

    **Division, `MOD`, `ROUND`, `TRUNCATE`, `AVG` and `SUM` now return
    `DECIMAL`.** They returned `DOUBLE`, or `BIGINT` for an integer `SUM`.
    Drivers hand a `DECIMAL` to your language differently — Python gets
    `decimal.Decimal` where it got `float`, PHP and Go get a string where they
    got a float. Code that assumes a float from `SUM()` or `AVG()` needs a
    conversion it did not need before.

    The scales follow MySQL exactly: `10 / 3` is `3.3333` (the dividend's scale
    plus four), `AVG` over `DECIMAL(12,2)` has scale 6, and `SUM` over an `INT`
    column is `DECIMAL` because the total can exceed the column's range.

    **A range bound written as a string now returns the rows it should.**

    ```sql
    SELECT COUNT(*) FROM t WHERE k > '1.5';   -- k INT: was missing row 2
    ```

    The bound was coerced to 2 while the strict `>` was kept, so row 2 was
    silently dropped. Any query comparing an indexed numeric column against a
    string literal — or against a scalar subquery whose result is rendered as
    text — was affected. Queries that returned too few rows now return the
    right ones.

!!! warning "Upgrading to 1.9.6"

    Three changes alter answers your client already reads, and one changes how
    the process fails. None touches your data.

    **Affected-row counts now match MySQL.** They were wrong in five shapes, and
    the 1-vs-2 distinction is how a client tells an insert from an update — so
    `updateOrCreate`-style code was reading the wrong answer and may contain a
    workaround that now reads the wrong answer in the other direction:

    | statement | was | now (and MySQL) |
    |---|---:|---:|
    | `INSERT ... ON DUPLICATE KEY UPDATE`, row updated | 1 | 2 |
    | ... set to the values it already had | 1 | 0 |
    | `REPLACE` replacing an existing row | 1 | 2 |
    | `UPDATE` that matched a row but changed nothing | 1 | 0 |

    **Search your code for anything that compensated for the old counts** before
    upgrading. Code that treats "affected == 1" as "inserted" will now see 2 for
    an update, which is the correct signal but the opposite conclusion.

    **A boolean used as a number is now an integer.** `SELECT TRUE + 1` returns
    `2` as `BIGINT` where it returned `2.0` as `DOUBLE`; likewise `(1 = 1) + 1`
    and `(2 > 1) + (3 > 2)`. Code asserting on the column type, or round-tripping
    through a typed language, sees the new type.

    **`CREATE VIEW` over an existing table name now reports 1050**
    (`ER_TABLE_EXISTS_ERROR`) instead of 1146 (`ER_NO_SUCH_TABLE`). 1146 was
    simply wrong; a migration tool that treats 1050 as "already done" will now
    behave correctly here.

    **The server aborts on panic instead of unwinding, so it must be
    supervised.** A panic is now a crash and a restart rather than a degraded
    process — which is the point: a panic that unwound while holding an internal
    lock left the server answering health checks while failing every query. The
    shipped `packaging/elyrasql.service` already sets `Restart=on-failure`. If
    you run the container or the binary under anything that does **not** restart
    it, add a restart policy before upgrading; see
    [Deployment](deployment.md#restart-policy).

!!! warning "Upgrading to 1.9.5"

    Four changes can affect a working deployment. None touches your data.

    **A partial cluster TLS configuration now refuses to start.** Setting only
    one of `ELYRASQL_CLUSTER_TLS_CERT` / `ELYRASQL_CLUSTER_TLS_KEY`, or pointing
    them at a certificate that cannot be loaded, previously logged a warning and
    continued **in plaintext**. That is now a startup error. If a node fails to
    start after upgrading, it was running unencrypted before: fix the pair rather
    than working around the error.

    **An exposed Raft control plane now requires authentication.** A control
    listener bound to anything other than loopback refuses to start unless
    `ELYRASQL_CLUSTER_SECRET` is set. To keep the previous behaviour
    deliberately, set `ELYRASQL_ALLOW_OPEN_AUTH=1`.

    **`SHOW PROCESSLIST` is scoped.** A non-`Admin` account now sees only its own
    connections. Monitoring that reads the full process list must connect as an
    `Admin` account.

    **Slow-query and audit output is redacted.** String literals are replaced with
    `?` and `CREATE USER` / `ALTER USER` / `SET PASSWORD` statements are elided
    entirely, so log pipelines that parsed literal values out of these lines will
    no longer find them. The audit log file is created owner-only (`0600`) on
    Unix.

    Two smaller behaviour changes worth knowing: `ELYRASQL_AUTH_LOCKOUT_SECS` is
    no longer read (the account lockout was removed — it let unauthenticated
    traffic lock out valid accounts), and queries that read a column-restricted
    table through a subquery, CTE or `GROUP BY` are now correctly refused where
    the restriction was previously bypassed.

!!! warning "Upgrading to 1.9.4"

    Two changes can affect a working deployment. Neither touches your data.

    **The container image has no shell.** It is now built `FROM scratch` — 22.3 MB
    down to 13.6 MB, with no OS packages and so no OS CVEs. But `docker exec
    <container> sh` no longer works, and neither does anything built on it:
    shell-based health checks, debugging one-liners, init wrappers. Check
    liveness over the MySQL protocol from outside the container instead:

    ```bash
    mysql -h 127.0.0.1 -P 3307 -u root -e 'SELECT 1'
    ```

    If you run the image under an orchestrator with an `exec`-based probe,
    change the probe before upgrading.

    **Result-column metadata reports different values.** Character columns now
    advertise their declared width under a utf8mb4 collation, matching MySQL: a
    `VARCHAR(32)` reports 128 bytes where it previously reported the unbounded
    text capacity, and clients that divide by the charset width now show 32
    instead of 21845. This is a fix — the old values were wrong in a way that
    made every client's length arithmetic wrong — but if you have code asserting
    on column lengths or collation ids, it will see new numbers. Row data,
    types and flags are unchanged.

    Tables created before 1.9.1 have no stored width and keep the unbounded
    value. Nothing needs to be rebuilt.

    Also: **`elyrasql serve` now exits on Ctrl-C.** If you deliberately started
    it with SIGINT ignored, installing the handler overrides that, and it will
    exit on a signal it used to survive.

!!! info "Upgrading to 1.9.2"

    Two fixes are the reason to upgrade promptly, because both were silent:

    - **`LEFT JOIN ... WHERE nullable IS NULL` returned the rows it should have
      excluded** when the `ON` had more than one condition. If you have results
      derived from that anti-join idiom, recompute them.
    - **`SET autocommit=0` was ignored**, so work committed immediately and
      `ROLLBACK` did nothing. Anything relying on it for transaction scope had
      no transaction.

    One change tightens validation: **`CHAR`/`VARCHAR` length limits are now
    enforced** in strict mode, so a string longer than its declared column is
    refused (22001) instead of stored. Declared types are only recorded for
    tables created from 1.9.2 onward, so existing tables are unaffected until
    recreated.

    Also worth knowing: a spilling `ORDER BY` — one over more than
    `ELYRASQL_SORT_MAX_ROWS` rows without a `LIMIT` — previously failed with an
    I/O error and now works. If you worked around that with a larger sort
    budget, you can put it back.

!!! info "Upgrading to 1.9.1"

    A patch release, but three of its fixes **tighten validation**, so statements
    that used to succeed can now fail:

    - a value too wide for its column (`300` into a `TINYINT`) raises 1264
    - a duplicate column name raises 1060, on `CREATE TABLE` and `ADD COLUMN`
    - `DELETE FROM t` on a self-referencing table without `ON DELETE CASCADE`
      raises 1451 instead of deleting the referenced rows

    Each of those matches MySQL, and each was previously accepted silently.
    Nothing is rewritten on upgrade: **integer widths are only enforced for
    tables created from 1.9.1 onward**, so an existing database cannot start
    rejecting data it already holds. A batched `INSERT` into a table with a
    self-referencing key now works, and a self-referencing `ON DELETE CASCADE`
    now follows the chain instead of leaving orphans.

!!! danger "Upgrade to 1.9.0 promptly"

    1.9.0 fixes two bugs that could return or write the wrong data **with no
    error**, and one that could take the server down. If you are on 1.8.0 or
    earlier, treat this as more than routine:

    - **`NATURAL JOIN` and `JOIN ... USING` executed as cross joins.** Every
      such query returned a cartesian product instead of a join. If you have
      results derived from those shapes, recompute them.
    - **A database qualifier was ignored**, so `UPDATE otherdb.t ...` and
      `DELETE FROM otherdb.t ...` modified the *local* table and reported
      success. Worth auditing if anything in your stack issues qualified writes
      — a migration runner pointed at the wrong environment, or a dump replayed
      with its original `db.table` names.
    - **Nested views could crash the server**, killing every connection.

    No on-disk format change and no migration: a 1.5.x through 1.8.x database
    opens unchanged. Relation aliases also become case-sensitive again (MySQL's
    behaviour), so `FROM t AS T WHERE t.id = 1` now errors where it used to be
    accepted.

!!! info "Upgrading to 1.8.0"

    No on-disk format change and no migration: a 1.5.x, 1.6.x or 1.7.x database
    opens unchanged. Two behaviour changes worth knowing. **`UNSIGNED` is now
    enforced on every integer width** — a `TINYINT`/`SMALLINT`/`INT UNSIGNED`
    column used to accept negative values, and now refuses them like
    `BIGINT UNSIGNED` always has (columns *created* by an older version keep the
    type they were created with, so they go on accepting negatives until the
    table is recreated). And several **error codes now match MySQL**: an unknown
    column is 1054 rather than 1146, and an out-of-range value is 1264 rather
    than 1366, so a client that branches on the code sees what it expects.

!!! info "Upgrading to 1.7.0"

    No on-disk format change and no migration: a 1.5.x or 1.6.x database opens
    unchanged, and a database written by 1.7.0 still opens in either. Two behaviour
    changes worth knowing before you upgrade. **`CREATE DATABASE` now refuses**
    instead of quietly succeeding, because this server has one logical schema and
    reporting success made callers believe otherwise; the conditional forms
    (`CREATE DATABASE IF NOT EXISTS`, `DROP DATABASE IF EXISTS`) are still no-ops, so
    Laravel migrations and container entrypoints are unaffected. And the **advertised
    MySQL version is now 8.0.12** rather than 8.0.0, which lets version-gated clients
    generate window-function SQL they previously suppressed.

!!! info "Upgrading to 1.6.0"

    No on-disk format change and no migration: a 1.5.x database opens unchanged, and a
    database written by 1.6.0 still opens in 1.5.x. One behaviour change worth knowing
    before you upgrade: a **non-equi join** (`ON a.id < b.id`, a `BETWEEN` band join)
    used to be refused once its intermediate result hit
    `ELYRASQL_JOIN_MAX_ROWS`/`_BYTES`. It now streams, so such a query *answers* —
    with bounded memory but `O(n x m)` time. If you relied on the ceilings to stop
    runaway joins, set [`ELYRASQL_QUERY_TIMEOUT_MS`](configuration.md) instead; it
    interrupts one promptly and leaves the session usable.

## Release binaries

Each [release](https://github.com/kwhorne/ElyraSQL/releases) ships fully static
Linux `musl` binaries for `x86_64` and `aarch64`, and — since v1.7.0 — a native
Apple Silicon macOS binary. The macOS build links only Apple-provided system
libraries and supports macOS 11 or later.

```bash
# Linux
VER=X.Y.Z
ARCH=x86_64   # or aarch64
curl -L -o elyrasql.tar.gz \
  "https://github.com/kwhorne/ElyraSQL/releases/download/v${VER}/elyrasql-${VER}-linux-${ARCH}.tar.gz"
tar xzf elyrasql.tar.gz
cd elyrasql-${VER}-linux-${ARCH}
./elyrasql version
```

```bash
# Apple Silicon, macOS 11+
VER=X.Y.Z
curl -L -o elyrasql.tar.gz \
  "https://github.com/kwhorne/ElyraSQL/releases/download/v${VER}/elyrasql-${VER}-macos-aarch64.tar.gz"
tar xzf elyrasql.tar.gz
cd elyrasql-${VER}-macos-aarch64
./elyrasql version
```

Every archive contains `elyrasql`, `README`, and `LICENSE`; Linux archives also
contain a sample `elyrasql.service` systemd unit. Verify integrity with the
published `.sha256` file (`sha256sum -c` on Linux or `shasum -a 256 -c` on
macOS).

!!! note "macOS code signing"

    The macOS binary currently has the ad-hoc signature produced by the Apple
    linker, not a Developer ID signature or Apple notarization. Environments
    that require Gatekeeper publisher verification should build from source
    until signed and notarized release credentials are configured.

## Docker

Multi-arch image (`amd64` + `arm64`) on the GitHub Container Registry:

```bash
docker pull ghcr.io/kwhorne/elyrasql:1.12.15   # or :latest
docker run -p 3307:3307 -v elyra:/var/lib/elyrasql ghcr.io/kwhorne/elyrasql:1.12.15
```

The image is ~15 MB, runs as a non-root user, stores data in the
`/var/lib/elyrasql` volume, and is configured via environment variables (see
[Configuration](configuration.md)).

## Build from source

Requires Rust 1.88+ and the platform toolchain. On macOS, install Xcode Command
Line Tools first (`xcode-select --install`).

```bash
git clone https://github.com/kwhorne/ElyraSQL.git
cd ElyraSQL
cargo build --release
./target/release/elyrasql serve
```

For an explicit Apple Silicon build with the same deployment floor as the
release:

```bash
rustup target add aarch64-apple-darwin
MACOSX_DEPLOYMENT_TARGET=11.0 \
  cargo build --release --locked --target aarch64-apple-darwin -p elyra-cli
./target/aarch64-apple-darwin/release/elyrasql version
```

To build a static binary yourself:

```bash
rustup target add x86_64-unknown-linux-musl
sudo apt-get install -y musl-tools
cargo build --release --target x86_64-unknown-linux-musl -p elyra-cli
```

## Systemd (Ubuntu)

The repository ships a hardened unit and an install script:

```bash
sudo ./packaging/deploy.sh
# or with credentials + TLS:
ELYRASQL_USER=root ELYRASQL_PASSWORD=secret \
  ELYRASQL_LISTEN=0.0.0.0:3307 sudo -E ./packaging/deploy.sh
```

See [Deployment](deployment.md) for details.
