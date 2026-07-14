//! Soak / chaos testing.
//!
//! These tests hammer a *real* `elyrasql` process with many concurrent
//! connections running a mixed read/write/transaction workload, and continuously
//! verify a global invariant. They target the class of bug that per-operation
//! tests can't reach: races between concurrent commits, and torn state after an
//! ungraceful crash under continuous writes.
//!
//! ## The bank invariant
//!
//! `N` accounts hold a fixed total balance. Workers run atomic transfers
//! (`BEGIN; UPDATE -amt WHERE bal >= amt; UPDATE +amt; COMMIT`), retrying on
//! write-write conflicts and reconnecting whenever the server restarts. Two
//! invariants must hold at *every* consistent snapshot:
//!
//!   1. `SUM(balance)` equals the initial total (no money created or destroyed),
//!   2. no balance is negative.
//!
//! Both are preserved by transaction *atomicity* alone, independent of the
//! durability mode -- so the same invariant also proves crash consistency: after
//! a `SIGKILL` mid-write, whatever survived is whole (both legs of every
//! surviving transfer committed together).
//!
//! ## Tuning
//!
//! Short by default so CI runs them as real coverage. Override for long runs:
//!   ELYRASQL_SOAK_SECS      test duration in seconds (default 6 / 10)
//!   ELYRASQL_SOAK_WORKERS   concurrent workers        (default 6)
//!   ELYRASQL_SOAK_ACCOUNTS  number of accounts        (default 16)
//!   ELYRASQL_SOAK_KILL_MS   chaos kill interval (ms)  (default 2500)

use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mysql_async::prelude::*;

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");
const INITIAL_BALANCE: i64 = 1000;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Cfg {
    secs: u64,
    workers: u64,
    accounts: i64,
    kill_ms: u64,
}

impl Cfg {
    fn load(default_secs: u64) -> Cfg {
        Cfg {
            secs: env_u64("ELYRASQL_SOAK_SECS", default_secs),
            workers: env_u64("ELYRASQL_SOAK_WORKERS", 6),
            accounts: env_u64("ELYRASQL_SOAK_ACCOUNTS", 16) as i64,
            kill_ms: env_u64("ELYRASQL_SOAK_KILL_MS", 2500),
        }
    }
    fn total(&self) -> i64 {
        self.accounts * INITIAL_BALANCE
    }
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn spawn(data: &std::path::Path, port: u16, sync_full: bool) -> Child {
    let mut cmd = Command::new(BIN);
    cmd.args(["serve", "--data"])
        .arg(data)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .env("RUST_LOG", "error");
    if sync_full {
        cmd.env("ELYRASQL_SYNC", "full");
    }
    cmd.spawn().expect("spawn elyrasql")
}

async fn wait_ready(port: u16) {
    for _ in 0..400 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server on port {port} never became ready");
}

fn opts(port: u16) -> mysql_async::Opts {
    mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .prefer_socket(false)
        .into()
}

async fn conn(port: u16) -> mysql_async::Conn {
    mysql_async::Conn::new(opts(port)).await.expect("connect")
}

/// Reconnect with backoff until the deadline (the server may be mid-restart).
async fn connect_retry(port: u16, deadline: Instant) -> Option<mysql_async::Conn> {
    while Instant::now() < deadline {
        if let Ok(c) = mysql_async::Conn::new(opts(port)).await {
            return Some(c);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

/// A server error means the server processed the statement (e.g. a write-write
/// conflict, 1213) -- retryable. Anything else (IO/driver/closed) means the
/// connection is gone and we must reconnect.
fn is_server_error(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(_))
}

enum Outcome {
    Committed,
    Skipped,
    Conflict,
    Disconnect,
}

async fn transfer(c: &mut mysql_async::Conn, a: i64, b: i64, amt: i64) -> Outcome {
    macro_rules! step {
        ($e:expr) => {
            match $e.await {
                Ok(_) => {}
                Err(e) => {
                    let _ = c.query_drop("ROLLBACK").await;
                    return if is_server_error(&e) {
                        Outcome::Conflict
                    } else {
                        Outcome::Disconnect
                    };
                }
            }
        };
    }
    if c.query_drop("START TRANSACTION").await.is_err() {
        return Outcome::Disconnect;
    }
    step!(c.query_drop(format!(
        "UPDATE accounts SET balance = balance - {amt} WHERE id = {a} AND balance >= {amt}"
    )));
    if c.affected_rows() == 0 {
        // insufficient funds -- nothing changed, unwind cleanly
        let _ = c.query_drop("ROLLBACK").await;
        return Outcome::Skipped;
    }
    step!(c.query_drop(format!(
        "UPDATE accounts SET balance = balance + {amt} WHERE id = {b}"
    )));
    match c.query_drop("COMMIT").await {
        Ok(_) => Outcome::Committed,
        Err(e) => {
            if is_server_error(&e) {
                Outcome::Conflict
            } else {
                Outcome::Disconnect
            }
        }
    }
}

/// Verify the two invariants against a consistent snapshot.
async fn check_invariant(c: &mut mysql_async::Conn, total: i64) -> Result<(), String> {
    let sum: Option<i64> = c
        .query_first("SELECT SUM(balance) FROM accounts")
        .await
        .map_err(|e| format!("SUM query failed: {e}"))?;
    let sum = sum.unwrap_or(-1);
    if sum != total {
        return Err(format!(
            "balance not conserved: SUM = {sum}, expected {total}"
        ));
    }
    let min: Option<i64> = c
        .query_first("SELECT MIN(balance) FROM accounts")
        .await
        .map_err(|e| format!("MIN query failed: {e}"))?;
    if min.unwrap_or(0) < 0 {
        return Err(format!("negative balance observed: MIN = {:?}", min));
    }
    Ok(())
}

/// Simple deterministic PRNG (no external crate).
fn next_rand(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}

async fn setup_accounts(c: &mut mysql_async::Conn, n: i64) {
    c.query_drop("DROP TABLE IF EXISTS accounts").await.unwrap();
    c.query_drop("CREATE TABLE accounts (id INT PRIMARY KEY, balance BIGINT NOT NULL)")
        .await
        .unwrap();
    let values: Vec<String> = (0..n).map(|i| format!("({i},{INITIAL_BALANCE})")).collect();
    c.query_drop(format!("INSERT INTO accounts VALUES {}", values.join(",")))
        .await
        .unwrap();
    let count: i64 = c
        .query_first("SELECT COUNT(*) FROM accounts")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, n, "account setup failed");
}

struct Stats {
    committed: Arc<AtomicU64>,
    conflicts: Arc<AtomicU64>,
    violation: Arc<Mutex<Option<String>>>,
}

impl Stats {
    fn new() -> Stats {
        Stats {
            committed: Arc::new(AtomicU64::new(0)),
            conflicts: Arc::new(AtomicU64::new(0)),
            violation: Arc::new(Mutex::new(None)),
        }
    }
    fn record_violation(&self, msg: String) {
        let mut v = self.violation.lock().unwrap();
        if v.is_none() {
            *v = Some(msg);
        }
    }
    fn take_violation(&self) -> Option<String> {
        self.violation.lock().unwrap().take()
    }
}

fn spawn_worker(
    port: u16,
    deadline: Instant,
    n: i64,
    seed: u64,
    stop: Arc<AtomicBool>,
    stats: &Stats,
) -> tokio::task::JoinHandle<()> {
    let committed = stats.committed.clone();
    let conflicts = stats.conflicts.clone();
    tokio::spawn(async move {
        let mut rng = seed;
        let mut c = match connect_retry(port, deadline).await {
            Some(c) => c,
            None => return,
        };
        while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
            let a = (next_rand(&mut rng) as i64) % n;
            let b = (a + 1 + (next_rand(&mut rng) as i64) % (n - 1)) % n;
            let amt = 1 + (next_rand(&mut rng) as i64) % 50;
            match transfer(&mut c, a, b, amt).await {
                Outcome::Committed => {
                    committed.fetch_add(1, Ordering::Relaxed);
                }
                Outcome::Conflict => {
                    conflicts.fetch_add(1, Ordering::Relaxed);
                }
                Outcome::Skipped => {}
                Outcome::Disconnect => {
                    // Server went away (chaos restart) -- reconnect.
                    match connect_retry(port, deadline).await {
                        Some(nc) => c = nc,
                        None => return,
                    }
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Test 1: sustained concurrency, no crashes.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_transfers_preserve_invariant() {
    let cfg = Cfg::load(6);
    let port = free_port();
    let data = std::env::temp_dir().join(format!("elyrasql-soak-c-{}.edb", std::process::id()));
    let _ = std::fs::remove_file(&data);

    let mut child = spawn(&data, port, false);
    wait_ready(port).await;

    {
        let mut c = conn(port).await;
        setup_accounts(&mut c, cfg.accounts).await;
    }

    let deadline = Instant::now() + Duration::from_secs(cfg.secs);
    let stop = Arc::new(AtomicBool::new(false));
    let stats = Stats::new();

    let workers: Vec<_> = (0..cfg.workers)
        .map(|w| {
            spawn_worker(
                port,
                deadline,
                cfg.accounts,
                0x9E37 + w * 2654435761,
                stop.clone(),
                &stats,
            )
        })
        .collect();

    // Continuously check the invariant while the workers hammer.
    let mut checker = conn(port).await;
    while Instant::now() < deadline {
        if let Err(msg) = check_invariant(&mut checker, cfg.total()).await {
            stats.record_violation(msg);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }

    // Final settled check.
    let mut c = conn(port).await;
    if let Err(msg) = check_invariant(&mut c, cfg.total()).await {
        stats.record_violation(msg);
    }

    let committed = stats.committed.load(Ordering::Relaxed);
    let conflicts = stats.conflicts.load(Ordering::Relaxed);
    eprintln!(
        "[soak] {} workers, {}s: {committed} transfers committed, {conflicts} conflicts retried",
        cfg.workers, cfg.secs
    );

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&data);

    if let Some(msg) = stats.take_violation() {
        panic!("invariant violated under concurrency: {msg}");
    }
    assert!(
        committed > 0,
        "no transfers committed -- workload did not run"
    );
}

// ---------------------------------------------------------------------------
// Test 2: sustained concurrency WITH random SIGKILL + restart.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_during_writes_preserves_invariant() {
    let cfg = Cfg::load(10);
    let port = free_port();
    let data = std::env::temp_dir().join(format!("elyrasql-soak-k-{}.edb", std::process::id()));
    let _ = std::fs::remove_file(&data);

    // fsync every commit so a surviving transfer is fully durable.
    let mut child = spawn(&data, port, true);
    wait_ready(port).await;

    {
        let mut c = conn(port).await;
        setup_accounts(&mut c, cfg.accounts).await;
    }

    let deadline = Instant::now() + Duration::from_secs(cfg.secs);
    let stop = Arc::new(AtomicBool::new(false));
    let stats = Stats::new();

    let workers: Vec<_> = (0..cfg.workers)
        .map(|w| {
            spawn_worker(
                port,
                deadline,
                cfg.accounts,
                0xABCD + w * 40503,
                stop.clone(),
                &stats,
            )
        })
        .collect();

    // Chaos loop: repeatedly SIGKILL the server mid-write, restart it, and
    // verify the invariant survived every crash.
    let mut kills = 0u32;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(cfg.kill_ms)).await;
        if Instant::now() >= deadline {
            break;
        }
        // Ungraceful crash.
        child.kill().expect("kill");
        let _ = child.wait();
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Restart on the same data file + port.
        child = spawn(&data, port, true);
        wait_ready(port).await;
        kills += 1;
        // The invariant must hold immediately after crash recovery.
        let mut c = match connect_retry(port, deadline + Duration::from_secs(5)).await {
            Some(c) => c,
            None => {
                stats.record_violation("could not connect after restart".into());
                break;
            }
        };
        if let Err(msg) = check_invariant(&mut c, cfg.total()).await {
            stats.record_violation(format!("after crash #{kills}: {msg}"));
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.await;
    }

    // Make sure the server is up for the final check.
    if child.try_wait().ok().flatten().is_some() {
        child = spawn(&data, port, true);
        wait_ready(port).await;
    }
    let mut c = conn(port).await;
    if let Err(msg) = check_invariant(&mut c, cfg.total()).await {
        stats.record_violation(format!("final: {msg}"));
    }

    let committed = stats.committed.load(Ordering::Relaxed);
    eprintln!(
        "[soak-chaos] {} workers, {}s, {kills} crashes: {committed} transfers committed",
        cfg.workers, cfg.secs
    );

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&data);

    if let Some(msg) = stats.take_violation() {
        panic!("invariant violated across crashes: {msg}");
    }
    assert!(kills > 0, "chaos loop never crashed the server");
    assert!(
        committed > 0,
        "no transfers committed -- workload did not run"
    );
}
