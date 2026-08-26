//! Embedded ElyraSQL in one file, with no server involved.
//!
//! ```text
//! cargo run -p elyra-embed --example basic            # throwaway database
//! cargo run -p elyra-embed --example basic /tmp/x.edb # a file that persists
//! ```
//!
//! A path given on the command line is created if missing and left behind, so
//! the same file can afterwards be served with `elyrasql serve --data <path>`.

use elyra_embed::{Database, Outcome, Value};

fn main() -> Result<(), elyra_embed::Error> {
    let db = match std::env::args().nth(1) {
        Some(path) => Database::open(path)?,
        None => Database::temporary()?,
    };
    println!("database: {}", db.path().display());

    let conn = db.connect();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS orders (
             id       INT PRIMARY KEY AUTO_INCREMENT,
             customer TEXT NOT NULL,
             total    DECIMAL(10,2) NOT NULL,
             placed   DATETIME
         )",
    )?;

    let inserted = conn.execute(
        "INSERT INTO orders (customer, total, placed) VALUES
             ('Ada',   1250.00, '2026-08-26 09:15:00'),
             ('Linus',  399.95, '2026-08-26 11:40:00'),
             ('Grace',  875.50, '2026-08-26 14:02:00')",
    )?;
    if let Outcome::Insert {
        affected_rows,
        last_insert_id,
    } = inserted[0]
    {
        println!("inserted {affected_rows} rows, first id {last_insert_id}");
    }

    // Exact DECIMAL arithmetic, evaluated by the same code the server runs:
    // the average is not a float, and it does not drift.
    let rows = conn.query(
        "SELECT customer, total, ROUND(total * 0.25, 2) AS deposit
           FROM orders
          ORDER BY total DESC",
    )?;

    println!("\n{:<10} {:>10} {:>10}", "customer", "total", "deposit");
    for row in rows.iter() {
        println!(
            "{:<10} {:>10} {:>10}",
            render(&row[0]),
            render(&row[1]),
            render(&row[2])
        );
    }

    let summary = conn.query("SELECT COUNT(*), SUM(total), AVG(total) FROM orders")?;
    println!(
        "\n{} orders, total {}, average {}",
        render(&summary.rows[0][0]),
        render(&summary.rows[0][1]),
        render(&summary.rows[0][2]),
    );

    Ok(())
}

/// `to_wire_string` is the same rendering the server sends over the MySQL text
/// protocol, so printed output matches what a `mysql` client would show. It
/// returns `None` for SQL NULL, which has no text form.
fn render(v: &Value) -> String {
    v.to_wire_string().unwrap_or_else(|| "NULL".to_string())
}
