/*
 * Embedded ElyraSQL from C.
 *
 *   cargo build -p elyra-embed-capi
 *   cc crates/elyra-embed-capi/examples/basic.c \
 *      -I crates/elyra-embed-capi/include \
 *      -L target/debug -lelyrasql -o /tmp/elyra_c_example
 *   /tmp/elyra_c_example
 */

#include <stdio.h>
#include <stdlib.h>
#include "elyrasql.h"

/* Every fallible call funnels through here, so the error path is written once. */
static void check(int rc, const char *what) {
    if (rc != ELYRA_OK) {
        const char *msg = elyra_last_error();
        fprintf(stderr, "%s failed: %s\n", what, msg ? msg : "(no message)");
        exit(1);
    }
}

int main(void) {
    printf("elyra-embed %s\n", elyra_version());

    ElyraDb *db = NULL;
    check(elyra_db_open_temporary(&db), "open");
    printf("database: %s\n\n", elyra_db_path(db));

    ElyraConn *conn = NULL;
    check(elyra_db_connect(db, &conn), "connect");

    check(elyra_conn_execute(conn,
        "CREATE TABLE orders ("
        "  id INT PRIMARY KEY AUTO_INCREMENT,"
        "  customer TEXT NOT NULL,"
        "  total DECIMAL(10,2) NOT NULL,"
        "  note TEXT)", NULL), "create table");

    uint64_t affected = 0;
    check(elyra_conn_execute(conn,
        "INSERT INTO orders (customer, total, note) VALUES"
        " ('Ada', 1250.00, 'priority'),"
        " ('Linus', 399.95, NULL),"
        " ('Grace', 875.50, 'gift wrap')", &affected), "insert");
    printf("inserted %llu rows, last id %lld\n\n",
           (unsigned long long)affected,
           (long long)elyra_conn_last_insert_id(conn));

    ElyraRows *rows = NULL;
    check(elyra_conn_query(conn,
        "SELECT customer, total, ROUND(total * 0.25, 2) AS deposit, note"
        "  FROM orders ORDER BY total DESC", &rows), "query");

    size_t ncols = elyra_rows_columns(rows);
    for (size_t c = 0; c < ncols; c++) {
        printf("%-12s", elyra_rows_column_name(rows, c));
    }
    printf("\n");

    size_t nrows = elyra_rows_count(rows);
    for (size_t r = 0; r < nrows; r++) {
        for (size_t c = 0; c < ncols; c++) {
            if (elyra_rows_is_null(rows, r, c) == 1) {
                printf("%-12s", "NULL");
            } else {
                printf("%-12s", elyra_rows_value(rows, r, c));
            }
        }
        printf("\n");
    }

    /* Out-of-range access is reported, not undefined. */
    printf("\nout-of-range is_null: %d (expected -1)\n",
           elyra_rows_is_null(rows, nrows, 0));

    /* An error leaves a message behind rather than crashing. */
    ElyraRows *bad = NULL;
    if (elyra_conn_query(conn, "SELECT * FROM no_such_table", &bad) != ELYRA_OK) {
        printf("expected error: %s\n", elyra_last_error());
    }

    elyra_rows_free(rows);
    elyra_conn_free(conn);
    elyra_db_free(db);
    return 0;
}
