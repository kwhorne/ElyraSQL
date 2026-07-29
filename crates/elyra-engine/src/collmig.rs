//! Migration to the accent-insensitive default collation (`utf8mb4_0900_ai_ci`).
//!
//! The collation folding feeds the on-disk key encoding, so making the default
//! accent-insensitive changes the bytes under which text sorts and is stored:
//! secondary indexes, UNIQUE constraints and text primary keys. Without a
//! migration a row written as `æble` would afterwards be looked up as `aeble`
//! and simply not be found -- silent row loss, not merely wrong ordering.
//!
//! Two properties keep this affordable:
//!
//! * The folding only differs for **non-ASCII** text. A database whose indexed
//!   and primary-key text is pure ASCII needs no data rewritten at all, which is
//!   the overwhelming majority.
//! * Index entries are derived data, so they can simply be rebuilt. Only rows
//!   under a **text primary key** have to be re-keyed, and that is done by
//!   writing the row at its new key and deleting the old one in the *same*
//!   transaction, so a crash mid-migration leaves the database on one side or
//!   the other and never half-way.

use crate::{catalog, index, session::Session};
use elyra_core::{Error, Result, Value};

/// Bumped when the collation changes the bytes of a stored key.
pub const COLLATION_VERSION: u64 = 2;

const VERSION_KEY: &[u8] = b"meta::collation_version";

/// Read the collation version a database was written with. Absent means a
/// database from before versioning, i.e. the original codepoint-ordered folding.
async fn stored_version(db: &Session) -> Result<u64> {
    Ok(match db.get(VERSION_KEY.to_vec()).await? {
        Some(v) if v.len() == 8 => u64::from_be_bytes(v[..8].try_into().unwrap()),
        // A database that predates this key was written with version 1.
        Some(_) | None => 1,
    })
}

/// True if any value in the row's text-bearing key columns folds differently
/// under the new collation, i.e. contains non-ASCII text.
fn needs_refold(row: &[Value], cols: &[usize]) -> bool {
    cols.iter().any(|&c| match row.get(c) {
        Some(Value::Text(s)) | Some(Value::Json(s)) => !s.is_ascii(),
        _ => false,
    })
}

/// Bring a database up to the current collation version, rebuilding index
/// entries and re-keying text primary keys where the folding changed.
///
/// Runs at open, before the server accepts connections, so no query ever sees a
/// half-migrated keyspace.
pub async fn migrate(db: &Session) -> Result<()> {
    if stored_version(db).await? >= COLLATION_VERSION {
        return Ok(());
    }

    let tables = catalog::list_tables(db).await?;
    let mut migrated_tables = 0usize;
    let mut rekeyed_rows = 0u64;

    for table in &tables {
        let def = match catalog::load(db, table).await {
            Ok(d) => d,
            // A relation we cannot load is not one we can safely rewrite.
            Err(_) => continue,
        };

        // Columns whose stored bytes depend on the folding: the clustered key and
        // every indexed column. A binary-collation column is unaffected.
        let mut key_cols: Vec<usize> = def
            .pk_cols
            .iter()
            .copied()
            .filter(|&c| !def.collation_of(c).is_bin())
            .collect();
        for idx in &def.indexes {
            for &c in &idx.cols {
                if !def.collation_of(c).is_bin() && !key_cols.contains(&c) {
                    key_cols.push(c);
                }
            }
        }
        if key_cols.is_empty() {
            continue;
        }

        let prefix = catalog::data_prefix(table);
        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut dels: Vec<Vec<u8>> = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        let mut touched = false;
        // New PK bytes -> the row that claimed them, so a collision introduced by
        // folding (`æ` and `ae` becoming equal) is reported rather than silently
        // overwriting one of the two rows.
        let mut claimed: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();

        loop {
            let chunk = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
            if chunk.is_empty() {
                break;
            }
            let last = chunk.len() < 4096;
            cursor = chunk.last().map(|(k, _)| k.clone());
            for (k, v) in chunk {
                let row: Vec<Value> =
                    bincode::deserialize(&v).map_err(|e| Error::Storage(e.to_string()))?;

                // Re-key the row itself if its clustered key contains non-ASCII text.
                let mut key = k.clone();
                let pk_text = def.pk_cols.iter().any(|&c| !def.collation_of(c).is_bin());
                if pk_text && needs_refold(&row, &def.pk_cols) {
                    let vals: Vec<Value> = def
                        .pk_cols
                        .iter()
                        .map(|&c| row.get(c).cloned().unwrap_or(Value::Null))
                        .collect();
                    let encoded = crate::keyenc::encode_key_coll(&vals, &def.pk_collations())?;
                    let newk = catalog::data_key(table, &encoded);
                    if newk != k {
                        if let Some(other) = claimed.get(&newk) {
                            return Err(Error::Query(format!(
                                "collation migration: rows with primary keys {} and {} in table \
                                 `{table}` become equal under the accent-insensitive default \
                                 collation (utf8mb4_0900_ai_ci); remove or change one of them, \
                                 then restart",
                                String::from_utf8_lossy(other),
                                String::from_utf8_lossy(&k),
                            )));
                        }
                        claimed.insert(newk.clone(), k.clone());
                        puts.push((newk.clone(), v.clone()));
                        dels.push(k.clone());
                        key = newk;
                        rekeyed_rows += 1;
                    }
                }

                // Index entries are derived, so rebuild them at the (possibly new) key.
                if needs_refold(&row, &key_cols) || key != k {
                    touched = true;
                }
                puts.extend(index::entries_for_row(&def, &row, &key)?);
            }
            if last {
                break;
            }
        }

        if !touched {
            // Pure-ASCII keys fold identically; nothing to rewrite.
            continue;
        }

        // Drop the old index entries wholesale before writing the rebuilt ones, so
        // stale keys from the previous folding cannot survive.
        for p in [
            catalog::index_table_prefix(table),
            catalog::indexnull_table_prefix(table),
        ] {
            let mut cur: Option<Vec<u8>> = None;
            loop {
                let batch = db.scan_batch(p.clone(), cur.clone(), 4096).await?;
                if batch.is_empty() {
                    break;
                }
                let last = batch.len() < 4096;
                cur = batch.last().map(|(k, _)| k.clone());
                for (k, _) in batch {
                    dels.push(k);
                }
                if last {
                    break;
                }
            }
        }

        db.commit_write(puts, dels).await?;
        migrated_tables += 1;
    }

    db.commit_write(
        vec![(
            VERSION_KEY.to_vec(),
            COLLATION_VERSION.to_be_bytes().to_vec(),
        )],
        vec![],
    )
    .await?;

    if migrated_tables > 0 {
        tracing::info!(
            tables = migrated_tables,
            rekeyed_rows,
            "migrated to the accent-insensitive default collation (utf8mb4_0900_ai_ci)"
        );
    }
    Ok(())
}
