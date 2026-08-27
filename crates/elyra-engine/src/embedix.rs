//! Declarative embedding indexes: the database keeps a vector column in step
//! with a text column, instead of the application orchestrating re-embedding.
//!
//! ```sql
//! CREATE EMBEDDING INDEX article_body ON articles(body) INTO embedding
//!     USING MODEL 'text-embedding-3-small' DIMENSION 1536;
//! ```
//!
//! After that, writing `body` is enough: the row is queued, embedded against the
//! configured provider, and its `embedding` column is filled in. `HYBRID()` and
//! `VEC_DISTANCE()` then work on it exactly as they do on a hand-maintained
//! vector column, because it *is* one.
//!
//! # Why the target column is explicit
//!
//! The obvious shorter form — `CREATE EMBEDDING INDEX ON articles(body)` with
//! the vector kept out of sight — was rejected. A hidden column cannot be named
//! by `HYBRID(body, 'q', embedding, …)` or `VEC_DISTANCE`, which is where all the
//! value is; and creating one implicitly would edit the user's schema behind
//! their back, changing what `SELECT *` returns and what `DESCRIBE` reports.
//! `INTO <column>` costs one clause and keeps the vector an ordinary column.
//!
//! # Why a separate catalog key
//!
//! [`crate::catalog::TableDef`] is bincode-encoded, and bincode is not
//! self-describing: a new encoded field makes every existing database
//! unreadable. So this lives under its own `embedix::` keyspace, the same way
//! `storage_generation`, `matview::` and `sys::colgrant::` do, and an existence
//! flag keeps tables without embedding indexes from paying a read per write.

use serde::{Deserialize, Serialize};

use elyra_core::{ColumnType, Error, Privilege, Result, Schema, Value};

use crate::catalog;
use crate::session::{Isolation, Session};
use crate::sqllex::{tokenize, Tok};
use crate::stream::RowStream;
use crate::QueryResult;

/// One declared embedding index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmbeddingIndex {
    pub name: String,
    pub table: String,
    /// Column whose text is embedded.
    pub text_col: String,
    /// `VECTOR(n)` column the embedding is written to.
    pub vec_col: String,
    /// Provider model. `None` uses the server default
    /// (`ELYRASQL_AI_EMBED_MODEL`), so a deployment can move models without
    /// rewriting its DDL.
    pub model: Option<String>,
    /// Declared dimension, cross-checked against `vec_col`'s type at creation.
    pub dimension: u32,
}

const PREFIX: &[u8] = b"embedix::";

/// `embedix::<table>::<name>`.
pub fn index_key(table: &str, name: &str) -> Vec<u8> {
    let mut k = PREFIX.to_vec();
    k.extend_from_slice(table.as_bytes());
    k.extend_from_slice(b"::");
    k.extend_from_slice(name.as_bytes());
    k
}

/// Prefix covering every embedding index on one table.
pub fn table_prefix(table: &str) -> Vec<u8> {
    let mut k = PREFIX.to_vec();
    k.extend_from_slice(table.as_bytes());
    k.extend_from_slice(b"::");
    k
}

fn decode(bytes: &[u8]) -> Result<EmbeddingIndex> {
    bincode::deserialize(bytes).map_err(|e| Error::Storage(e.to_string()))
}

fn encode(index: &EmbeddingIndex) -> Result<Vec<u8>> {
    bincode::serialize(index).map_err(|e| Error::Storage(e.to_string()))
}

/// One index by table and name.
pub async fn load(sess: &Session, table: &str, name: &str) -> Result<Option<EmbeddingIndex>> {
    match sess.get(index_key(table, name)).await? {
        Some(b) => Ok(Some(decode(&b)?)),
        None => Ok(None),
    }
}

/// Every embedding index on a table, ordered by name.
pub async fn list_for_table(sess: &Session, table: &str) -> Result<Vec<EmbeddingIndex>> {
    scan(sess, table_prefix(table)).await
}

/// Every embedding index in the database, ordered by table then name.
pub async fn list_all(sess: &Session) -> Result<Vec<EmbeddingIndex>> {
    scan(sess, PREFIX.to_vec()).await
}

async fn scan(sess: &Session, prefix: Vec<u8>) -> Result<Vec<EmbeddingIndex>> {
    let mut out = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let batch = sess.scan_batch(prefix.clone(), cursor.clone(), 256).await?;
        if batch.is_empty() {
            break;
        }
        let short = batch.len() < 256;
        cursor = batch.last().map(|(k, _)| k.clone());
        for (_, v) in batch {
            out.push(decode(&v)?);
        }
        if short {
            break;
        }
    }
    Ok(out)
}

// --- Statement handling -----------------------------------------------------

/// Whether this statement is handled here rather than by the SQL frontend.
///
/// Byte comparison, not slicing: `h[..kw.len()]` panics when the offset lands
/// inside a multi-byte character, which any statement with non-ASCII text can do.
pub fn is_embedding_stmt(head: &str) -> bool {
    let h = head.trim_start();
    let starts = |kw: &str| {
        let (hb, kb) = (h.as_bytes(), kw.as_bytes());
        hb.len() >= kb.len() && hb[..kb.len()].eq_ignore_ascii_case(kb)
    };
    starts("create embedding index")
        || starts("drop embedding index")
        || starts("show embedding index")
}

/// Execute a `CREATE`/`DROP`/`SHOW EMBEDDING INDEX` statement.
pub async fn execute(sql: &str, sess: &Session, privilege: Privilege) -> Result<QueryResult> {
    let toks = tokenize(sql);
    let word = |i: usize| {
        toks.get(i)
            .map(|t| t.text().to_string())
            .unwrap_or_default()
    };

    if toks.first().map(|t| t.is_word("show")).unwrap_or(false) {
        return show(&toks, sess).await;
    }

    // CREATE and DROP change the catalog.
    if privilege < Privilege::Admin {
        return Err(Error::Query(
            "embedding index DDL requires Admin privileges".into(),
        ));
    }

    if word(0).eq_ignore_ascii_case("create") {
        create(&toks, sess).await
    } else {
        drop_index(&toks, sess).await
    }
}

/// `CREATE EMBEDDING INDEX [IF NOT EXISTS] <name> ON <table>(<text_col>)
///  INTO <vec_col> [USING MODEL '<model>'] [DIMENSION <n>]`
async fn create(toks: &[Tok], sess: &Session) -> Result<QueryResult> {
    let mut i = 3; // create embedding index
    let mut if_not_exists = false;
    if toks.get(i).map(|t| t.is_word("if")).unwrap_or(false)
        && toks.get(i + 1).map(|t| t.is_word("not")).unwrap_or(false)
        && toks
            .get(i + 2)
            .map(|t| t.is_word("exists"))
            .unwrap_or(false)
    {
        if_not_exists = true;
        i += 3;
    }

    let name = ident(toks, i, "index name")?;
    i += 1;
    expect_word(toks, i, "on")?;
    i += 1;
    let table = ident(toks, i, "table name")?;
    i += 1;

    expect_sym(toks, i, '(')?;
    i += 1;
    let text_col = ident(toks, i, "source column")?;
    i += 1;
    expect_sym(toks, i, ')')?;
    i += 1;

    expect_word(toks, i, "into")?;
    i += 1;
    let vec_col = ident(toks, i, "target vector column")?;
    i += 1;

    let mut model = None;
    if toks.get(i).map(|t| t.is_word("using")).unwrap_or(false) {
        i += 1;
        expect_word(toks, i, "model")?;
        i += 1;
        model = Some(ident(toks, i, "model name")?);
        i += 1;
    }
    let mut declared_dimension: Option<u32> = None;
    if toks.get(i).map(|t| t.is_word("dimension")).unwrap_or(false) {
        i += 1;
        let raw = ident(toks, i, "dimension")?;
        declared_dimension = Some(raw.parse::<u32>().map_err(|_| {
            Error::Query(format!("DIMENSION must be a positive integer, got {raw:?}"))
        })?);
    }

    let definition = catalog::load(sess, &table).await?;
    let (text_ci, vec_ci) = validate_columns(&definition.schema, &table, &text_col, &vec_col)?;
    let ColumnType::Vector(column_dimension) = definition.schema.columns[vec_ci].ty else {
        unreachable!("validate_columns checked the type");
    };
    // A declared DIMENSION is a cross-check, not a second source of truth: the
    // column already carries the real one, and silently disagreeing with it
    // would produce vectors the column cannot store.
    if let Some(declared) = declared_dimension {
        if declared != column_dimension {
            return Err(Error::Query(format!(
                "DIMENSION {declared} does not match {vec_col} VECTOR({column_dimension})"
            )));
        }
    }
    let _ = text_ci;

    if load(sess, &table, &name).await?.is_some() {
        if if_not_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Query(format!(
            "embedding index {name} already exists on {table}"
        )));
    }

    let index = EmbeddingIndex {
        name: name.clone(),
        table: table.clone(),
        text_col,
        vec_col,
        model,
        dimension: column_dimension,
    };
    sess.commit_write(vec![(index_key(&table, &name), encode(&index)?)], vec![])
        .await?;
    Ok(QueryResult::Affected(0))
}

/// `DROP EMBEDDING INDEX [IF EXISTS] <name> ON <table>`
async fn drop_index(toks: &[Tok], sess: &Session) -> Result<QueryResult> {
    let mut i = 3; // drop embedding index
    let mut if_exists = false;
    if toks.get(i).map(|t| t.is_word("if")).unwrap_or(false)
        && toks
            .get(i + 1)
            .map(|t| t.is_word("exists"))
            .unwrap_or(false)
    {
        if_exists = true;
        i += 2;
    }
    let name = ident(toks, i, "index name")?;
    i += 1;
    expect_word(toks, i, "on")?;
    i += 1;
    let table = ident(toks, i, "table name")?;

    if load(sess, &table, &name).await?.is_none() {
        if if_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Query(format!(
            "no embedding index {name} on {table}"
        )));
    }
    sess.commit_write(vec![], vec![index_key(&table, &name)])
        .await?;
    Ok(QueryResult::Affected(0))
}

/// `SHOW EMBEDDING INDEXES [ON <table>]`
async fn show(toks: &[Tok], sess: &Session) -> Result<QueryResult> {
    let indexes = if toks.get(3).map(|t| t.is_word("on")).unwrap_or(false) {
        list_for_table(sess, &ident(toks, 4, "table name")?).await?
    } else {
        list_all(sess).await?
    };

    let mut statuses = Vec::with_capacity(indexes.len());
    for index in &indexes {
        statuses.push(status(sess, index).await?);
    }

    let schema = Schema::new(
        [
            "Name",
            "Table",
            "Source",
            "Target",
            "Model",
            "Dimension",
            "Retrying",
            "Failed",
        ]
        .iter()
        .map(|n| elyra_core::ColumnDef {
            name: (*n).into(),
            ty: if matches!(*n, "Dimension" | "Retrying" | "Failed") {
                ColumnType::Int
            } else {
                ColumnType::Text
            },
            nullable: *n == "Model",
            collation: elyra_core::Collation::Ci,
            qualifier: Vec::new(),
            result_metadata: Default::default(),
        })
        .collect(),
    );
    let rows = indexes
        .into_iter()
        .zip(statuses)
        .map(|(ix, st)| {
            vec![
                Value::Text(ix.name),
                Value::Text(ix.table),
                Value::Text(ix.text_col),
                Value::Text(ix.vec_col),
                ix.model.map(Value::Text).unwrap_or(Value::Null),
                Value::Int(ix.dimension as i64),
                Value::Int(st.retrying as i64),
                Value::Int(st.failed as i64),
            ]
        })
        .collect();
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// Check that the source column holds text and the target is a vector column,
/// returning both schema indices.
fn validate_columns(
    schema: &Schema,
    table: &str,
    text_col: &str,
    vec_col: &str,
) -> Result<(usize, usize)> {
    let find = |name: &str| {
        schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    };
    let text_ci =
        find(text_col).ok_or_else(|| Error::UnknownColumn(format!("{table}.{text_col}")))?;
    let vec_ci = find(vec_col).ok_or_else(|| Error::UnknownColumn(format!("{table}.{vec_col}")))?;

    if text_ci == vec_ci {
        return Err(Error::Query(
            "the source and target of an embedding index must be different columns".into(),
        ));
    }
    // JSON is accepted alongside TEXT: a document rendered as text is a
    // perfectly ordinary thing to embed, and it is stored as its text form.
    if !matches!(
        schema.columns[text_ci].ty,
        ColumnType::Text | ColumnType::Json
    ) {
        return Err(Error::Query(format!(
            "{text_col} is {} — an embedding index reads a TEXT or JSON column",
            schema.columns[text_ci].ty.display_name()
        )));
    }
    if !matches!(schema.columns[vec_ci].ty, ColumnType::Vector(_)) {
        return Err(Error::Query(format!(
            "{vec_col} is {} — an embedding index writes to a VECTOR column",
            schema.columns[vec_ci].ty.display_name()
        )));
    }
    Ok((text_ci, vec_ci))
}

fn ident(toks: &[Tok], i: usize, what: &str) -> Result<String> {
    match toks.get(i) {
        Some(Tok::Word(s)) | Some(Tok::Str(s)) if !s.is_empty() => Ok(s.clone()),
        _ => Err(Error::Query(format!("expected {what}"))),
    }
}

fn expect_word(toks: &[Tok], i: usize, kw: &str) -> Result<()> {
    if toks.get(i).map(|t| t.is_word(kw)).unwrap_or(false) {
        Ok(())
    } else {
        Err(Error::Query(format!("expected {}", kw.to_uppercase())))
    }
}

fn expect_sym(toks: &[Tok], i: usize, sym: char) -> Result<()> {
    if matches!(toks.get(i), Some(Tok::Sym(c)) if *c == sym) {
        Ok(())
    } else {
        Err(Error::Query(format!("expected {sym}")))
    }
}

// --- Keeping the vector in step with the text -------------------------------
//
// Nothing hooks INSERT/UPDATE. What needs embedding is *derived* from the data:
// a row is up to date when the hash of (model, text) matches the hash recorded
// when its vector was written. That is the same shape `vindex::reconcile` uses,
// and it is robust to every path that writes rows -- bulk loads, restores,
// binlog replay, replication apply -- none of which a write hook would see.

/// Per-row bookkeeping under `embedh::<table>::<index>::<rowkey>`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RowState {
    /// Hash of the (model, text) pair that produced the vector now in the row.
    /// Zero when nothing has been embedded yet.
    pub hash: u64,
    /// Consecutive failures. Reset on success, and on any change to the text.
    pub attempts: u32,
    /// Wall-clock milliseconds before which no retry is attempted.
    pub next_attempt_ms: u64,
    pub last_error: Option<String>,
}

/// Failures after which a row stops being retried until its text changes.
///
/// A permanently unembeddable row -- input the provider rejects, a quota that
/// is not coming back -- must stop burning requests and money, while staying
/// visible rather than silently dropped. Editing the text changes the hash and
/// clears the state, which is the natural way to un-stick one.
pub const MAX_ATTEMPTS: u32 = 5;

const STATE_PREFIX: &[u8] = b"embedh::";

fn state_key(table: &str, index: &str, row_key: &[u8]) -> Vec<u8> {
    let mut k = STATE_PREFIX.to_vec();
    k.extend_from_slice(table.as_bytes());
    k.extend_from_slice(b"::");
    k.extend_from_slice(index.as_bytes());
    k.extend_from_slice(b"::");
    k.extend_from_slice(row_key);
    k
}

/// Stable content hash of the (model, text) pair.
///
/// SHA-256 rather than `DefaultHasher`, because this value is **persisted**:
/// `DefaultHasher` gives no cross-version stability guarantee, and a hash that
/// shifted under a toolchain upgrade would silently re-embed every row in the
/// database -- an expensive way to change nothing. `sha2` is already a
/// dependency for the `SHA2()` SQL function.
fn content_hash(model: Option<&str>, text: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(model.unwrap_or("").as_bytes());
    h.update([0u8]); // domain separator: model and text cannot run together
    h.update(text.as_bytes());
    let digest = h.finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("sha256 is 32 bytes"))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Exponential backoff, capped: 1s, 2s, 4s, 8s, 16s.
fn backoff_ms(attempts: u32) -> u64 {
    1000u64 << attempts.min(4)
}

/// What one sweep did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SweepReport {
    /// Rows embedded and written.
    pub embedded: u64,
    /// Rows whose embedding call failed this sweep.
    pub failed: u64,
    /// Rows skipped because a concurrent write changed them mid-flight. They
    /// are picked up by the next sweep; nothing is lost.
    pub conflicted: u64,
    /// Rows that need embedding but were not attempted: budget exhausted, still
    /// inside their backoff window, or dead-lettered past [`MAX_ATTEMPTS`].
    pub deferred: u64,
}

/// The future an embedder hands back. Boxed because the sweeper holds embedders
/// behind a trait object, which cannot name an opaque future type.
pub type EmbedFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<f32>>> + Send>>;

/// How the sweeper turns text into a vector: `(text, model) -> vector`.
///
/// Injectable so the sweep logic can be tested without a provider, a network or
/// a process-global environment variable. Production passes [`live_embedder`].
pub type Embedder<'a> = &'a (dyn Fn(String, Option<String>) -> EmbedFuture + Sync);

/// The real embedder: an OpenAI-compatible endpoint, via [`crate::aiembed`].
pub fn live_embedder() -> impl Fn(String, Option<String>) -> EmbedFuture + Sync {
    |text: String, model: Option<String>| {
        Box::pin(async move { crate::aiembed::embed_with(&text, model.as_deref()).await })
    }
}

/// Bring one embedding index up to date, embedding at most `budget` rows.
///
/// Returns without touching the provider when nothing needs work, so calling it
/// on an idle table is a scan and no more.
pub async fn sweep_index(
    sess: &Session,
    index: &EmbeddingIndex,
    budget: usize,
    embed: Embedder<'_>,
) -> Result<SweepReport> {
    let definition = catalog::load(sess, &index.table).await?;
    let (text_ci, vec_ci) = validate_columns(
        &definition.schema,
        &index.table,
        &index.text_col,
        &index.vec_col,
    )?;

    let mut report = SweepReport::default();
    let prefix = definition.data_prefix();
    let mut cursor: Option<Vec<u8>> = None;
    let now = now_ms();

    loop {
        let batch = sess.scan_batch(prefix.clone(), cursor.clone(), 256).await?;
        if batch.is_empty() {
            break;
        }
        let last_batch = batch.len() < 256;
        cursor = batch.last().map(|(k, _)| k.clone());

        for (row_key, encoded) in batch {
            let row = crate::rowdec::decode_row(&encoded)?;
            let Some(text) = row_text(row.get(text_ci)) else {
                continue; // NULL or empty: nothing to embed
            };
            let hash = content_hash(index.model.as_deref(), &text);

            let skey = state_key(&index.table, &index.name, &row_key);
            let state: RowState = match sess.get(skey.clone()).await? {
                Some(b) => bincode::deserialize(&b).unwrap_or_default(),
                None => RowState::default(),
            };
            if state.hash == hash && state.attempts == 0 {
                continue; // up to date
            }
            // A changed text resets a dead-lettered row: the input the provider
            // rejected is gone, so the old verdict no longer applies.
            let changed = state.hash != hash;
            let attempts = if changed { 0 } else { state.attempts };
            if !changed && (attempts >= MAX_ATTEMPTS || now < state.next_attempt_ms) {
                report.deferred += 1;
                continue;
            }
            if report.embedded as usize + report.failed as usize >= budget {
                report.deferred += 1;
                continue;
            }

            match embed(text.clone(), index.model.clone()).await {
                Ok(vector) => {
                    if vector.len() != index.dimension as usize {
                        record_failure(
                            sess,
                            &skey,
                            hash,
                            attempts,
                            format!(
                                "model returned {} dimensions, {} expects {}",
                                vector.len(),
                                index.vec_col,
                                index.dimension
                            ),
                        )
                        .await?;
                        report.failed += 1;
                        continue;
                    }
                    let target = PendingWrite {
                        table: &index.table,
                        row_key: &row_key,
                        expected: &encoded,
                        vec_ci,
                        state_key: &skey,
                        hash,
                    };
                    match write_vector(sess, &target, vector).await {
                        Ok(()) => report.embedded += 1,
                        Err(Error::Conflict(_)) => report.conflicted += 1,
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => {
                    record_failure(sess, &skey, hash, attempts, e.to_string()).await?;
                    report.failed += 1;
                }
            }
        }
        if last_batch {
            break;
        }
    }
    Ok(report)
}

/// The text to embed, or `None` when there is nothing worth sending.
fn row_text(value: Option<&Value>) -> Option<String> {
    let text = match value? {
        Value::Text(s) | Value::Json(s) => s.clone(),
        Value::Null => return None,
        _ => return None,
    };
    (!text.trim().is_empty()).then_some(text)
}

/// One row the sweep is ready to update, and everything needed to do it safely.
#[derive(Clone, Copy)]
struct PendingWrite<'a> {
    table: &'a str,
    row_key: &'a [u8],
    /// The row exactly as it was read, for the compare-and-swap below.
    expected: &'a [u8],
    /// Schema index of the vector column.
    vec_ci: usize,
    state_key: &'a [u8],
    hash: u64,
}

/// Write the vector into the row and record the hash, but only if the row is
/// byte-for-byte as it was read.
///
/// The sweep reads a row, calls a provider that may take a second, then writes
/// the row back. Without the check, a concurrent `UPDATE` landing in that window
/// would be silently overwritten by a stale copy. A serializable transaction
/// validates the read set at commit, so a racing write turns into a conflict --
/// the row simply stays pending for the next sweep.
async fn write_vector(sess: &Session, target: &PendingWrite<'_>, vector: Vec<f32>) -> Result<()> {
    let PendingWrite {
        table,
        row_key,
        expected,
        vec_ci,
        state_key,
        hash,
    } = *target;
    let previous_isolation = sess.transaction_isolation();
    sess.set_isolation(Isolation::Serializable);
    sess.begin()?;

    let result = async {
        let Some(current) = sess.get(row_key.to_vec()).await? else {
            // Deleted while we were embedding: drop the sidecar with it.
            sess.commit_write(vec![], vec![state_key.to_vec()]).await?;
            return Ok(());
        };
        if current != expected {
            return Err(Error::Conflict("row changed during embedding".into()));
        }
        let mut row = crate::rowdec::decode_row(&current)?;
        if vec_ci >= row.len() {
            return Err(Error::Conflict("row shape changed during embedding".into()));
        }
        row[vec_ci] = Value::Vector(vector);
        let encoded = bincode::serialize(&row).map_err(|e| Error::Storage(e.to_string()))?;
        let state = RowState {
            hash,
            attempts: 0,
            next_attempt_ms: 0,
            last_error: None,
        };
        // Advance the table's write counter in the same transaction. The HNSW
        // vector index reconciles only when this moves, so a vector written
        // without it is invisible to `VEC_DISTANCE` and to the vector half of
        // `HYBRID` -- which is the entire point of writing it. Reading the
        // counter inside the transaction puts it in the validated read set, so
        // two sweeps racing on the same table conflict instead of losing a bump.
        let wcount = crate::vindex::read_wcount(sess, table).await? + 1;
        sess.commit_write(
            vec![
                (row_key.to_vec(), encoded),
                (
                    state_key.to_vec(),
                    bincode::serialize(&state).map_err(|e| Error::Storage(e.to_string()))?,
                ),
                (
                    crate::catalog::wcount_key(table),
                    wcount.to_le_bytes().to_vec(),
                ),
            ],
            vec![],
        )
        .await
    }
    .await;

    let outcome = match result {
        Ok(()) => sess.commit().await,
        Err(e) => {
            sess.rollback();
            Err(e)
        }
    };
    let _ = sess.set_transaction_isolation(&previous_isolation);
    outcome
}

/// Record a failed attempt without touching the row.
async fn record_failure(
    sess: &Session,
    state_key: &[u8],
    hash: u64,
    attempts: u32,
    error: String,
) -> Result<()> {
    let attempts = attempts.saturating_add(1);
    let state = RowState {
        // Always the *current* hash. Recording anything else would make the next
        // sweep read the row as changed, reset the attempt counter and retry
        // immediately -- defeating both the backoff and the dead-letter cap. With
        // it recorded, an actual edit to the text is still detected as a change,
        // which is what clears a dead-lettered row.
        hash,
        attempts,
        next_attempt_ms: now_ms() + backoff_ms(attempts),
        last_error: Some(error),
    };
    sess.commit_write(
        vec![(
            state_key.to_vec(),
            bincode::serialize(&state).map_err(|e| Error::Storage(e.to_string()))?,
        )],
        vec![],
    )
    .await
}

// --- Driving the sweep ------------------------------------------------------

/// How many rows one index may embed per sweep, so a large backfill cannot
/// monopolise the provider (or the bill) in a single pass.
pub const SWEEP_BUDGET: usize = 256;

/// Sweep every embedding index in the database once.
///
/// Returns `None` without touching storage when no provider is configured. That
/// is deliberately not a failure: with no `ELYRASQL_AI_EMBED_URL`, every row
/// would "fail" without a single network call, and five sweeps later the whole
/// table would be dead-lettered by a deployment setting rather than by anything
/// wrong with the data.
pub async fn sweep_all(sess: &Session, embed: Embedder<'_>) -> Result<Option<SweepReport>> {
    if !crate::aiembed::is_configured() {
        return Ok(None);
    }
    let mut total = SweepReport::default();
    for index in list_all(sess).await? {
        match sweep_index(sess, &index, SWEEP_BUDGET, embed).await {
            Ok(report) => {
                total.embedded += report.embedded;
                total.failed += report.failed;
                total.conflicted += report.conflicted;
                total.deferred += report.deferred;
            }
            // One broken index -- a dropped table, a column that changed type --
            // must not stop the others.
            Err(error) => {
                tracing::warn!(%error, index = %index.name, table = %index.table,
                    "embedding index sweep failed");
            }
        }
    }
    Ok(Some(total))
}

/// Row counts per index, for `SHOW EMBEDDING INDEXES`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct IndexStatus {
    /// Rows that have failed at least once and are still being retried.
    pub retrying: u64,
    /// Rows past [`MAX_ATTEMPTS`], no longer retried until their text changes.
    pub failed: u64,
}

/// Read the recorded per-row state for one index.
pub async fn status(sess: &Session, index: &EmbeddingIndex) -> Result<IndexStatus> {
    let mut prefix = STATE_PREFIX.to_vec();
    prefix.extend_from_slice(index.table.as_bytes());
    prefix.extend_from_slice(b"::");
    prefix.extend_from_slice(index.name.as_bytes());
    prefix.extend_from_slice(b"::");

    let mut out = IndexStatus::default();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let batch = sess.scan_batch(prefix.clone(), cursor.clone(), 512).await?;
        if batch.is_empty() {
            break;
        }
        let short = batch.len() < 512;
        cursor = batch.last().map(|(k, _)| k.clone());
        for (_, v) in batch {
            let state: RowState = bincode::deserialize(&v).unwrap_or_default();
            if state.attempts >= MAX_ATTEMPTS {
                out.failed += 1;
            } else if state.attempts > 0 {
                out.retrying += 1;
            }
        }
        if short {
            break;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Engine, QueryResult};

    /// A database with one table shaped for embedding: a text source and a
    /// `VECTOR(4)` target.
    async fn fixture() -> (Engine, crate::Session) {
        let engine = Engine::new(elyra_storage::Db::in_memory().unwrap());
        let session = engine.session();
        engine
            .execute(
                "CREATE TABLE articles (id INT PRIMARY KEY, body TEXT, tags INT, embedding VECTOR(4))",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        (engine, session)
    }

    async fn run(engine: &Engine, session: &crate::Session, sql: &str) -> Result<Vec<QueryResult>> {
        engine.execute(sql, Privilege::Admin, session).await
    }

    /// Drain a single result set into rows.
    async fn rows_of(mut results: Vec<QueryResult>) -> Vec<Vec<Value>> {
        match results.remove(0) {
            QueryResult::Rows(mut stream) => {
                let mut out = Vec::new();
                loop {
                    let batch = stream.next_batch(256).await.unwrap();
                    if batch.is_empty() {
                        break;
                    }
                    out.extend(batch);
                }
                out
            }
            _ => panic!("expected a result set"),
        }
    }

    const CREATE: &str = "CREATE EMBEDDING INDEX body_ix ON articles(body) INTO embedding \
                          USING MODEL 'text-embedding-3-small' DIMENSION 4";

    #[tokio::test]
    async fn create_then_show_and_drop() {
        let (engine, session) = fixture().await;
        run(&engine, &session, CREATE).await.unwrap();

        let stored = load(&session, "articles", "body_ix")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.text_col, "body");
        assert_eq!(stored.vec_col, "embedding");
        assert_eq!(stored.model.as_deref(), Some("text-embedding-3-small"));
        // Taken from the column, not from the DIMENSION clause.
        assert_eq!(stored.dimension, 4);

        let listed = rows_of(
            run(&engine, &session, "SHOW EMBEDDING INDEXES")
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0][0], Value::Text("body_ix".into()));
        assert_eq!(listed[0][5], Value::Int(4));
        assert_eq!(listed[0][6], Value::Int(0), "nothing retrying yet");
        assert_eq!(listed[0][7], Value::Int(0), "nothing failed yet");

        run(
            &engine,
            &session,
            "DROP EMBEDDING INDEX body_ix ON articles",
        )
        .await
        .unwrap();
        assert!(load(&session, "articles", "body_ix")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn the_model_clause_is_optional() {
        let (engine, session) = fixture().await;
        run(
            &engine,
            &session,
            "CREATE EMBEDDING INDEX ix ON articles(body) INTO embedding",
        )
        .await
        .unwrap();
        let stored = load(&session, "articles", "ix").await.unwrap().unwrap();
        // None means "whatever the server is configured with", so a deployment
        // can change models without rewriting its DDL.
        assert_eq!(stored.model, None);
        assert_eq!(stored.dimension, 4);
    }

    #[tokio::test]
    async fn conditional_forms_are_no_ops() {
        let (engine, session) = fixture().await;
        run(&engine, &session, CREATE).await.unwrap();

        assert!(
            run(&engine, &session, CREATE).await.is_err(),
            "plain re-create"
        );
        let conditional = CREATE.replacen(
            "CREATE EMBEDDING INDEX",
            "CREATE EMBEDDING INDEX IF NOT EXISTS",
            1,
        );
        run(&engine, &session, &conditional).await.unwrap();

        run(
            &engine,
            &session,
            "DROP EMBEDDING INDEX body_ix ON articles",
        )
        .await
        .unwrap();
        assert!(
            run(
                &engine,
                &session,
                "DROP EMBEDDING INDEX body_ix ON articles"
            )
            .await
            .is_err(),
            "plain re-drop"
        );
        run(
            &engine,
            &session,
            "DROP EMBEDDING INDEX IF EXISTS body_ix ON articles",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn columns_are_validated_against_the_table() {
        let (engine, session) = fixture().await;
        let cases = [
            // Unknown columns on either side.
            "CREATE EMBEDDING INDEX ix ON articles(nope) INTO embedding",
            "CREATE EMBEDDING INDEX ix ON articles(body) INTO nope",
            // The source must hold text; `tags` is an INT.
            "CREATE EMBEDDING INDEX ix ON articles(tags) INTO embedding",
            // The target must be a vector; `body` is TEXT.
            "CREATE EMBEDDING INDEX ix ON articles(body) INTO body",
            // A declared dimension that contradicts the column.
            "CREATE EMBEDDING INDEX ix ON articles(body) INTO embedding DIMENSION 1536",
        ];
        for sql in cases {
            assert!(
                run(&engine, &session, sql).await.is_err(),
                "should have been rejected: {sql}"
            );
            assert!(
                load(&session, "articles", "ix").await.unwrap().is_none(),
                "a rejected statement must leave no catalog entry: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn ddl_needs_admin_but_show_does_not() {
        let (engine, session) = fixture().await;
        run(&engine, &session, CREATE).await.unwrap();

        let reader = engine.session();
        assert!(engine
            .execute(CREATE, Privilege::Read, &reader)
            .await
            .is_err());
        assert!(engine
            .execute(
                "DROP EMBEDDING INDEX body_ix ON articles",
                Privilege::Write,
                &reader
            )
            .await
            .is_err());
        assert!(engine
            .execute("SHOW EMBEDDING INDEXES", Privilege::Read, &reader)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn show_can_be_scoped_to_one_table() {
        let (engine, session) = fixture().await;
        run(
            &engine,
            &session,
            "CREATE TABLE notes (id INT PRIMARY KEY, memo TEXT, vec VECTOR(4))",
        )
        .await
        .unwrap();
        run(&engine, &session, CREATE).await.unwrap();
        run(
            &engine,
            &session,
            "CREATE EMBEDDING INDEX memo_ix ON notes(memo) INTO vec",
        )
        .await
        .unwrap();

        let all = rows_of(
            run(&engine, &session, "SHOW EMBEDDING INDEXES")
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(all.len(), 2);

        let scoped = rows_of(
            run(&engine, &session, "SHOW EMBEDDING INDEXES ON notes")
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0][1], Value::Text("notes".into()));
    }

    #[tokio::test]
    async fn a_json_column_may_be_the_source() {
        let (engine, session) = fixture().await;
        run(
            &engine,
            &session,
            "CREATE TABLE docs (id INT PRIMARY KEY, payload JSON, vec VECTOR(4))",
        )
        .await
        .unwrap();
        run(
            &engine,
            &session,
            "CREATE EMBEDDING INDEX payload_ix ON docs(payload) INTO vec",
        )
        .await
        .unwrap();
        assert!(load(&session, "docs", "payload_ix")
            .await
            .unwrap()
            .is_some());
    }

    #[test]
    fn only_embedding_statements_are_claimed() {
        assert!(is_embedding_stmt(
            "CREATE EMBEDDING INDEX ix ON t(a) INTO v"
        ));
        assert!(is_embedding_stmt("  drop embedding index ix on t"));
        assert!(is_embedding_stmt("SHOW EMBEDDING INDEXES"));
        assert!(!is_embedding_stmt("CREATE INDEX ix ON t (a)"));
        assert!(!is_embedding_stmt("SELECT 1"));
        // Multi-byte input must not panic on the prefix compare.
        assert!(!is_embedding_stmt("SELECT 'æ' = 'ae'"));
        assert!(!is_embedding_stmt("æøå"));
    }
    // --- Sweeping ----------------------------------------------------------

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// An embedder that never touches the network: it returns a fixed vector and
    /// counts calls, or fails on demand.
    #[derive(Clone, Default)]
    struct FakeProvider {
        calls: Arc<AtomicUsize>,
        fail: Arc<std::sync::atomic::AtomicBool>,
        dimension: usize,
    }

    impl FakeProvider {
        fn new(dimension: usize) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                fail: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                dimension,
            }
        }
        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
        fn set_failing(&self, failing: bool) {
            self.fail.store(failing, Ordering::Relaxed);
        }
        fn embedder(&self) -> impl Fn(String, Option<String>) -> EmbedFuture + Sync {
            let calls = self.calls.clone();
            let fail = self.fail.clone();
            let dimension = self.dimension;
            move |_text, _model| {
                calls.fetch_add(1, Ordering::Relaxed);
                let failing = fail.load(Ordering::Relaxed);
                Box::pin(async move {
                    if failing {
                        Err(Error::Query("provider unavailable".into()))
                    } else {
                        Ok(vec![0.25f32; dimension])
                    }
                })
            }
        }
    }

    async fn seeded() -> (Engine, crate::Session, EmbeddingIndex) {
        let (engine, session) = fixture().await;
        run(&engine, &session, CREATE).await.unwrap();
        run(
            &engine,
            &session,
            "INSERT INTO articles (id, body, tags) VALUES (1, 'privacy law', 0), (2, 'tax law', 0)",
        )
        .await
        .unwrap();
        let index = load(&session, "articles", "body_ix")
            .await
            .unwrap()
            .unwrap();
        (engine, session, index)
    }

    async fn vectors(engine: &Engine, session: &crate::Session) -> Vec<Value> {
        rows_of(
            run(
                engine,
                session,
                "SELECT embedding FROM articles ORDER BY id",
            )
            .await
            .unwrap(),
        )
        .await
        .into_iter()
        .map(|mut r| r.remove(0))
        .collect()
    }

    #[tokio::test]
    async fn a_sweep_fills_empty_vectors_and_then_does_nothing() {
        let (engine, session, index) = seeded().await;
        let provider = FakeProvider::new(4);

        let report = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(report.embedded, 2);
        assert_eq!(report.failed, 0);
        assert_eq!(provider.calls(), 2);

        let stored = vectors(&engine, &session).await;
        assert_eq!(stored[0], Value::Vector(vec![0.25; 4]));
        assert_eq!(stored[1], Value::Vector(vec![0.25; 4]));

        // The second sweep must be free: the hashes already match.
        let again = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(again, SweepReport::default());
        assert_eq!(
            provider.calls(),
            2,
            "an idle sweep must not call the provider"
        );
    }

    #[tokio::test]
    async fn changing_the_text_re_embeds_only_that_row() {
        let (engine, session, index) = seeded().await;
        let provider = FakeProvider::new(4);
        sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(provider.calls(), 2);

        run(
            &engine,
            &session,
            "UPDATE articles SET body = 'privacy and data protection' WHERE id = 1",
        )
        .await
        .unwrap();

        let report = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(report.embedded, 1, "only the edited row");
        assert_eq!(provider.calls(), 3);
    }

    #[tokio::test]
    async fn rows_with_no_text_are_never_sent() {
        let (engine, session, index) = seeded().await;
        run(
            &engine,
            &session,
            "INSERT INTO articles (id, body, tags) VALUES (3, NULL, 0), (4, '   ', 0)",
        )
        .await
        .unwrap();

        let provider = FakeProvider::new(4);
        let report = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(report.embedded, 2, "only the two rows that have text");
        assert_eq!(
            provider.calls(),
            2,
            "NULL and whitespace must not become provider requests"
        );
    }

    #[tokio::test]
    async fn a_budget_bounds_one_sweep() {
        let (engine, session, index) = seeded().await;
        let provider = FakeProvider::new(4);

        let report = sweep_index(&session, &index, 1, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(report.embedded, 1);
        assert_eq!(report.deferred, 1, "the rest is left for the next sweep");
        assert_eq!(provider.calls(), 1);

        let rest = sweep_index(&session, &index, 1, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(rest.embedded, 1);
        let _ = engine;
    }

    /// The bug this pins: recording anything but the current hash on failure
    /// makes the next sweep see the row as *changed*, which resets the attempt
    /// counter — so backoff never engages and a failing provider is hammered.
    #[tokio::test]
    async fn failures_back_off_and_eventually_stop() {
        let (_engine, session, index) = seeded().await;
        let provider = FakeProvider::new(4);
        provider.set_failing(true);

        let report = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(report.failed, 2);
        assert_eq!(report.embedded, 0);

        // Immediately re-sweeping must not retry: both rows are inside their
        // backoff window.
        let calls_after_first = provider.calls();
        let second = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(second.deferred, 2);
        assert_eq!(second.failed, 0);
        assert_eq!(
            provider.calls(),
            calls_after_first,
            "a row inside its backoff window must not be re-sent"
        );

        // Drive it to the dead-letter cap by clearing the backoff each round.
        for _ in 0..MAX_ATTEMPTS {
            clear_backoff(&session, &index).await;
            sweep_index(&session, &index, 100, &provider.embedder())
                .await
                .unwrap();
        }
        clear_backoff(&session, &index).await;
        let exhausted = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(
            exhausted.failed, 0,
            "past the cap, a row stops costing provider requests"
        );
        assert_eq!(exhausted.deferred, 2);

        // The failure is retained, not discarded.
        let state = row_states(&session, &index).await;
        assert!(state.iter().all(|s| s.attempts >= MAX_ATTEMPTS));
        assert!(state
            .iter()
            .all(|s| s.last_error.as_deref() == Some("query error: provider unavailable")));
    }

    #[tokio::test]
    async fn editing_the_text_revives_a_dead_lettered_row() {
        let (engine, session, index) = seeded().await;
        let provider = FakeProvider::new(4);
        provider.set_failing(true);
        for _ in 0..=MAX_ATTEMPTS {
            clear_backoff(&session, &index).await;
            sweep_index(&session, &index, 100, &provider.embedder())
                .await
                .unwrap();
        }
        assert!(row_states(&session, &index)
            .await
            .iter()
            .all(|s| s.attempts >= MAX_ATTEMPTS));

        provider.set_failing(false);
        run(
            &engine,
            &session,
            "UPDATE articles SET body = 'rewritten' WHERE id = 1",
        )
        .await
        .unwrap();

        let report = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(
            report.embedded, 1,
            "the edited row is reconsidered; the untouched one stays dead-lettered"
        );
        assert_eq!(report.deferred, 1);
    }

    #[tokio::test]
    async fn a_wrong_dimension_is_recorded_not_written() {
        let (engine, session, index) = seeded().await;
        // The column is VECTOR(4); this provider answers with 3.
        let provider = FakeProvider::new(3);

        let report = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(report.failed, 2);
        assert_eq!(report.embedded, 0);
        assert!(vectors(&engine, &session)
            .await
            .iter()
            .all(|v| *v == Value::Null));
        assert!(row_states(&session, &index).await.iter().all(|s| s
            .last_error
            .as_deref()
            .unwrap_or("")
            .contains("dimensions")));
    }

    /// Clear the backoff window so a test can drive attempts without sleeping.
    async fn clear_backoff(sess: &crate::Session, index: &EmbeddingIndex) {
        let prefix = {
            let mut k = STATE_PREFIX.to_vec();
            k.extend_from_slice(index.table.as_bytes());
            k.extend_from_slice(b"::");
            k.extend_from_slice(index.name.as_bytes());
            k.extend_from_slice(b"::");
            k
        };
        let batch = sess.scan_batch(prefix, None, 1024).await.unwrap();
        let mut puts = Vec::new();
        for (k, v) in batch {
            let mut state: RowState = bincode::deserialize(&v).unwrap();
            state.next_attempt_ms = 0;
            puts.push((k, bincode::serialize(&state).unwrap()));
        }
        sess.commit_write(puts, vec![]).await.unwrap();
    }

    async fn row_states(sess: &crate::Session, index: &EmbeddingIndex) -> Vec<RowState> {
        let mut prefix = STATE_PREFIX.to_vec();
        prefix.extend_from_slice(index.table.as_bytes());
        prefix.extend_from_slice(b"::");
        prefix.extend_from_slice(index.name.as_bytes());
        prefix.extend_from_slice(b"::");
        sess.scan_batch(prefix, None, 1024)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, v)| bincode::deserialize(&v).unwrap())
            .collect()
    }

    #[test]
    fn the_content_hash_covers_the_model() {
        // Two indexes on the same text with different models must not consider
        // each other's work done.
        assert_ne!(
            content_hash(Some("model-a"), "text"),
            content_hash(Some("model-b"), "text")
        );
        // And the separator keeps model and text from running together.
        assert_ne!(content_hash(Some("ab"), "c"), content_hash(Some("a"), "bc"));
    }

    /// The property that matters most: an embedding call takes real time, and a
    /// row can be updated inside that window. Writing the vector back blind
    /// would silently discard the user's edit.
    ///
    /// Made deterministic by having the "provider" perform the racing update
    /// itself, which puts the write exactly where a real race would put it:
    /// after the row was read, before the vector is stored.
    #[tokio::test]
    async fn a_concurrent_update_is_not_overwritten() {
        let (engine, session, index) = seeded().await;

        let racer = engine.clone();
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let embedder = {
            let fired = fired.clone();
            let calls = calls.clone();
            move |_text: String, _model: Option<String>| {
                let racer = racer.clone();
                let fired = fired.clone();
                calls.fetch_add(1, Ordering::Relaxed);
                Box::pin(async move {
                    if !fired.swap(true, Ordering::SeqCst) {
                        let other = racer.session();
                        racer
                            .execute(
                                "UPDATE articles SET body = 'edited mid-flight' WHERE id = 1",
                                Privilege::Admin,
                                &other,
                            )
                            .await
                            .unwrap();
                    }
                    Ok(vec![0.5f32; 4])
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<f32>>> + Send>>
            }
        };

        let report = sweep_index(&session, &index, 100, &embedder).await.unwrap();
        assert_eq!(report.conflicted, 1, "the raced row must not be written");
        assert_eq!(
            report.embedded, 1,
            "the untouched row still gets its vector"
        );

        // The edit survived.
        let bodies = rows_of(
            run(&engine, &session, "SELECT body FROM articles ORDER BY id")
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(bodies[0][0], Value::Text("edited mid-flight".into()));

        // And the row is still pending, so the next sweep embeds the new text.
        let after = sweep_index(&session, &index, 100, &embedder).await.unwrap();
        assert_eq!(after.embedded, 1);
        assert_eq!(after.conflicted, 0);
    }

    #[tokio::test]
    async fn show_reports_retrying_and_failed_counts() {
        let (engine, session, index) = seeded().await;
        let provider = FakeProvider::new(4);
        provider.set_failing(true);

        sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        let listed = rows_of(
            run(&engine, &session, "SHOW EMBEDDING INDEXES")
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(listed[0][6], Value::Int(2), "both rows are retrying");
        assert_eq!(listed[0][7], Value::Int(0));

        for _ in 0..=MAX_ATTEMPTS {
            clear_backoff(&session, &index).await;
            sweep_index(&session, &index, 100, &provider.embedder())
                .await
                .unwrap();
        }
        let listed = rows_of(
            run(&engine, &session, "SHOW EMBEDDING INDEXES")
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(listed[0][6], Value::Int(0));
        assert_eq!(listed[0][7], Value::Int(2), "both rows are dead-lettered");
    }

    /// An unconfigured provider is a deployment state, not a data problem. If a
    /// sweep treated it as a per-row failure, five passes would dead-letter the
    /// whole table without a single network call having been made.
    #[tokio::test]
    async fn no_configured_provider_means_no_sweep() {
        let (_engine, session, index) = seeded().await;
        let provider = FakeProvider::new(4);

        // `sweep_all` consults the real configuration, which is absent here.
        let outcome = sweep_all(&session, &provider.embedder()).await.unwrap();
        assert!(
            outcome.is_none(),
            "with no provider configured, nothing should be swept"
        );
        assert_eq!(provider.calls(), 0);
        assert_eq!(
            status(&session, &index).await.unwrap(),
            IndexStatus::default(),
            "and no row should have been marked as failing"
        );
    }
    /// A vector the sweep writes must be visible to vector search.
    ///
    /// The HNSW index rebuilds only when the table's write counter moves, and
    /// the sweep writes rows straight to storage rather than through the DML
    /// path that normally advances it. Without the bump the feature silently
    /// does nothing useful: the column fills in, and `VEC_DISTANCE`/`HYBRID`
    /// keep answering from an index that has never seen those vectors. Caught
    /// against a live provider, where a quantum-physics query ranked a document
    /// about a cat above the one about qubits.
    #[tokio::test]
    async fn a_written_vector_advances_the_tables_write_counter() {
        let (_engine, session, index) = seeded().await;
        let before = crate::vindex::read_wcount(&session, "articles")
            .await
            .unwrap();

        let provider = FakeProvider::new(4);
        let report = sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(report.embedded, 2);

        let after = crate::vindex::read_wcount(&session, "articles")
            .await
            .unwrap();
        assert!(
            after > before,
            "the write counter must advance so the vector index reconciles \
             (was {before}, now {after})"
        );

        // A sweep that embeds nothing must leave it alone, or every idle sweep
        // would invalidate the index it just built.
        let idle_before = after;
        sweep_index(&session, &index, 100, &provider.embedder())
            .await
            .unwrap();
        assert_eq!(
            crate::vindex::read_wcount(&session, "articles")
                .await
                .unwrap(),
            idle_before,
            "an idle sweep must not churn the vector index"
        );
    }
}
