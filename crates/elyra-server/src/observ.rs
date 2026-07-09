//! Observability: server metrics, a process list, and slow-query logging.
//!
//! Metrics and the process list are server-side concerns (they track wire
//! connections and query timing), so they live here rather than in the SQL
//! engine. `SHOW STATUS` / `SHOW PROCESSLIST` read this state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tracing::warn;

/// Coarse classification of a statement for `Com_*` counters.
fn classify(sql: &str) -> &'static str {
    let s = sql.trim_start();
    let kw: String = s
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_lowercase();
    match kw.as_str() {
        "select" | "with" | "show" | "describe" | "desc" | "explain" => "select",
        "insert" | "replace" => "insert",
        "update" => "update",
        "delete" | "truncate" => "delete",
        _ => "other",
    }
}

/// Process-wide counters, MySQL `SHOW STATUS`-style.
pub struct Metrics {
    start: Instant,
    questions: AtomicU64,
    com_select: AtomicU64,
    com_insert: AtomicU64,
    com_update: AtomicU64,
    com_delete: AtomicU64,
    com_other: AtomicU64,
    errors: AtomicU64,
    slow: AtomicU64,
    conns_current: AtomicU64,
    conns_total: AtomicU64,
    slow_ms: u128,
}

impl Metrics {
    pub fn new(slow_ms: u128) -> Self {
        Metrics {
            start: Instant::now(),
            questions: AtomicU64::new(0),
            com_select: AtomicU64::new(0),
            com_insert: AtomicU64::new(0),
            com_update: AtomicU64::new(0),
            com_delete: AtomicU64::new(0),
            com_other: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            slow: AtomicU64::new(0),
            conns_current: AtomicU64::new(0),
            conns_total: AtomicU64::new(0),
            slow_ms,
        }
    }

    pub fn connect(&self) {
        self.conns_current.fetch_add(1, Ordering::Relaxed);
        self.conns_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn disconnect(&self) {
        self.conns_current.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a completed query: bump counters, and slow-log if over threshold.
    pub fn record(&self, sql: &str, ok: bool, elapsed: Duration) {
        self.questions.fetch_add(1, Ordering::Relaxed);
        let counter = match classify(sql) {
            "select" => &self.com_select,
            "insert" => &self.com_insert,
            "update" => &self.com_update,
            "delete" => &self.com_delete,
            _ => &self.com_other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        if self.slow_ms > 0 && elapsed.as_millis() >= self.slow_ms {
            self.slow.fetch_add(1, Ordering::Relaxed);
            warn!(
                duration_ms = elapsed.as_millis() as u64,
                sql = %truncate(sql, 500),
                "slow query"
            );
        }
    }

    /// Render all counters in the Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut s = String::with_capacity(1024);
        let gauge = |s: &mut String, name: &str, help: &str, v: u64| {
            s.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n"
            ));
        };
        let counter = |s: &mut String, name: &str, help: &str, v: u64| {
            s.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}\n"
            ));
        };
        gauge(
            &mut s,
            "elyrasql_uptime_seconds",
            "Seconds since the server started",
            self.start.elapsed().as_secs(),
        );
        gauge(
            &mut s,
            "elyrasql_connections_current",
            "Currently open connections",
            g(&self.conns_current),
        );
        counter(
            &mut s,
            "elyrasql_connections_total",
            "Total connections since start",
            g(&self.conns_total),
        );
        counter(
            &mut s,
            "elyrasql_questions_total",
            "Total statements executed",
            g(&self.questions),
        );
        // Per-command counters as a labelled family.
        s.push_str(
            "# HELP elyrasql_commands_total Statements executed by type\n\
             # TYPE elyrasql_commands_total counter\n",
        );
        for (label, a) in [
            ("select", &self.com_select),
            ("insert", &self.com_insert),
            ("update", &self.com_update),
            ("delete", &self.com_delete),
            ("other", &self.com_other),
        ] {
            s.push_str(&format!(
                "elyrasql_commands_total{{command=\"{label}\"}} {}\n",
                g(a)
            ));
        }
        counter(
            &mut s,
            "elyrasql_errors_total",
            "Statements that returned an error",
            g(&self.errors),
        );
        counter(
            &mut s,
            "elyrasql_slow_queries_total",
            "Statements at or above the slow threshold",
            g(&self.slow),
        );
        s
    }

    /// `(Variable_name, Value)` rows for `SHOW STATUS`.
    pub fn status_rows(&self) -> Vec<(String, String)> {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed).to_string();
        vec![
            ("Uptime".into(), self.start.elapsed().as_secs().to_string()),
            ("Threads_connected".into(), g(&self.conns_current)),
            ("Connections".into(), g(&self.conns_total)),
            ("Questions".into(), g(&self.questions)),
            ("Queries".into(), g(&self.questions)),
            ("Com_select".into(), g(&self.com_select)),
            ("Com_insert".into(), g(&self.com_insert)),
            ("Com_update".into(), g(&self.com_update)),
            ("Com_delete".into(), g(&self.com_delete)),
            ("Com_other".into(), g(&self.com_other)),
            ("Errors".into(), g(&self.errors)),
            ("Slow_queries".into(), g(&self.slow)),
        ]
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

/// One connection's live state, shown by `SHOW PROCESSLIST`.
struct ProcEntry {
    user: String,
    host: String,
    connected_at: Instant,
    /// The currently executing statement and when it started, if any.
    current: Option<(String, Instant)>,
}

/// Registry of live connections.
pub struct ProcRegistry {
    inner: Mutex<HashMap<u32, ProcEntry>>,
}

impl Default for ProcRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcRegistry {
    pub fn new() -> Self {
        ProcRegistry {
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(&self, id: u32, host: String) {
        self.inner.lock().unwrap().insert(
            id,
            ProcEntry {
                user: String::new(),
                host,
                connected_at: Instant::now(),
                current: None,
            },
        );
    }

    pub fn set_user(&self, id: u32, user: String) {
        if let Some(e) = self.inner.lock().unwrap().get_mut(&id) {
            e.user = user;
        }
    }

    pub fn begin_query(&self, id: u32, sql: &str) {
        if let Some(e) = self.inner.lock().unwrap().get_mut(&id) {
            e.current = Some((truncate(sql, 1000), Instant::now()));
        }
    }

    pub fn end_query(&self, id: u32) {
        if let Some(e) = self.inner.lock().unwrap().get_mut(&id) {
            e.current = None;
        }
    }

    pub fn deregister(&self, id: u32) {
        self.inner.lock().unwrap().remove(&id);
    }

    /// Rows for `SHOW PROCESSLIST`: Id, User, Host, db, Command, Time, State, Info.
    pub fn rows(&self) -> Vec<Vec<String>> {
        let g = self.inner.lock().unwrap();
        let mut ids: Vec<&u32> = g.keys().collect();
        ids.sort();
        ids.into_iter()
            .map(|id| {
                let e = &g[id];
                let (command, secs, info) = match &e.current {
                    Some((sql, since)) => ("Query", since.elapsed().as_secs(), sql.clone()),
                    None => ("Sleep", e.connected_at.elapsed().as_secs(), String::new()),
                };
                vec![
                    id.to_string(),
                    if e.user.is_empty() {
                        "unauthenticated".into()
                    } else {
                        e.user.clone()
                    },
                    e.host.clone(),
                    String::new(), // db (single catalog)
                    command.into(),
                    secs.to_string(),
                    String::new(), // state
                    info,
                ]
            })
            .collect()
    }
}

/// Milliseconds since the Unix epoch (used only for coarse diagnostics).
#[allow(dead_code)]
pub fn epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
