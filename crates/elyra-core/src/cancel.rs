//! Cooperative cancellation for statement execution.
//!
//! A `tokio::time::timeout` around a query can only take effect at an `.await`
//! point, so it cannot stop a long stretch of synchronous CPU work: the future is
//! never polled again until that stretch finishes, and work already handed to a
//! blocking thread keeps running even after the caller has given up. The practical
//! consequence is that a wall-clock timeout alone does not free the CPU.
//!
//! The engine therefore checks a shared token at cheap intervals inside its hot
//! row loops. When the deadline has passed (or the statement was cancelled) the
//! loop stops and the query returns an error, releasing the thread it was using.
//!
//! Checks must be nearly free, since they sit in per-row loops: callers hold a
//! [`CancelCheck`], which only consults the clock once every
//! [`CHECK_INTERVAL`] rows.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::Error;

/// Rows processed between two clock reads. Large enough that the check is
/// negligible per row, small enough that a query stops promptly.
pub const CHECK_INTERVAL: u32 = 1024;

/// Process-wide monotonic origin, so a deadline can live in an atomic as a plain
/// nanosecond offset (an `Instant` is not atomically storable).
fn origin() -> Instant {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    *ORIGIN.get_or_init(Instant::now)
}

fn now_ns() -> u64 {
    origin().elapsed().as_nanos() as u64
}

/// Shared cancellation state for the statement currently running on a session.
#[derive(Debug, Default)]
pub struct QueryCancel {
    /// Deadline in nanoseconds since [`origin`]; `0` means "no deadline".
    deadline_ns: AtomicU64,
    /// Set when the statement was cancelled explicitly rather than timing out.
    cancelled: AtomicBool,
    /// The configured timeout, kept only so the error message can quote it.
    timeout_ms: AtomicU64,
}

impl QueryCancel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start a statement: apply `timeout` (if any) and clear any previous
    /// cancellation. Called once per statement, never in a hot loop.
    pub fn arm(&self, timeout: Option<Duration>) {
        self.cancelled.store(false, Ordering::Relaxed);
        match timeout {
            Some(d) => {
                self.timeout_ms
                    .store(d.as_millis() as u64, Ordering::Relaxed);
                // Saturate rather than wrap: an absurd timeout simply never fires.
                let at = now_ns().saturating_add(d.as_nanos().min(u64::MAX as u128) as u64);
                self.deadline_ns.store(at.max(1), Ordering::Relaxed);
            }
            None => {
                self.timeout_ms.store(0, Ordering::Relaxed);
                self.deadline_ns.store(0, Ordering::Relaxed);
            }
        }
    }

    /// Whether a deadline is currently set. Used to let a nested statement (a
    /// trigger body, a procedure) inherit the outer statement's deadline instead
    /// of granting itself a fresh budget.
    pub fn is_armed(&self) -> bool {
        self.deadline_ns.load(Ordering::Relaxed) != 0
    }

    /// End a statement: no deadline applies between statements.
    pub fn disarm(&self) {
        self.deadline_ns.store(0, Ordering::Relaxed);
        self.cancelled.store(false, Ordering::Relaxed);
    }

    /// Ask the running statement to stop as soon as it reaches its next check.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Whether execution should stop. Reads the clock, so callers in row loops
    /// should go through [`CancelCheck`] instead of calling this per row.
    pub fn should_stop(&self) -> bool {
        if self.cancelled.load(Ordering::Relaxed) {
            return true;
        }
        let deadline = self.deadline_ns.load(Ordering::Relaxed);
        deadline != 0 && now_ns() >= deadline
    }

    /// `Err` when execution should stop, with a message explaining which limit
    /// ended the statement.
    pub fn check(&self) -> Result<(), Error> {
        if !self.should_stop() {
            return Ok(());
        }
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(Error::Query("query cancelled".into()));
        }
        Err(Error::Query(format!(
            "query exceeded ELYRASQL_QUERY_TIMEOUT_MS ({} ms)",
            self.timeout_ms.load(Ordering::Relaxed)
        )))
    }
}

/// A per-loop cancellation checker: counts rows locally and only consults the
/// shared token (and the clock) every [`CHECK_INTERVAL`] rows.
///
/// Holding the counter locally rather than in the token keeps the common case to
/// an increment and a branch, with no atomic contention between concurrent
/// queries.
pub struct CancelCheck {
    token: Arc<QueryCancel>,
    tick: u32,
}

impl CancelCheck {
    pub fn new(token: Arc<QueryCancel>) -> Self {
        Self { token, tick: 0 }
    }

    /// Count one row and, at the sampling interval, stop the query if its
    /// deadline has passed.
    #[inline]
    pub fn tick(&mut self) -> Result<(), Error> {
        self.tick = self.tick.wrapping_add(1);
        if self.tick % CHECK_INTERVAL == 0 {
            return self.token.check();
        }
        Ok(())
    }

    /// Check immediately, ignoring the sampling interval. For loops whose
    /// iterations are individually expensive (a merge pass, a partition).
    #[inline]
    pub fn tick_now(&mut self) -> Result<(), Error> {
        self.token.check()
    }
}

/// Per-statement timeout from `ELYRASQL_QUERY_TIMEOUT_MS`, shared by the server
/// (which also unblocks the client) and the engine (which stops the work).
pub fn query_timeout() -> Option<Duration> {
    static CACHE: OnceLock<Option<Duration>> = OnceLock::new();
    *CACHE.get_or_init(|| {
        std::env::var("ELYRASQL_QUERY_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .map(Duration::from_millis)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unarmed_token_never_stops() {
        let t = QueryCancel::new();
        assert!(!t.should_stop());
        assert!(t.check().is_ok());
        t.arm(None);
        assert!(!t.should_stop());
    }

    #[test]
    fn expired_deadline_stops_with_a_timeout_message() {
        let t = QueryCancel::new();
        t.arm(Some(Duration::from_millis(1)));
        std::thread::sleep(Duration::from_millis(15));
        assert!(t.should_stop());
        let err = t.check().unwrap_err().to_string();
        assert!(err.contains("ELYRASQL_QUERY_TIMEOUT_MS"), "got: {err}");
        // Disarming ends the statement, so the token is reusable.
        t.disarm();
        assert!(!t.should_stop());
    }

    #[test]
    fn explicit_cancel_is_reported_as_cancelled() {
        let t = QueryCancel::new();
        t.arm(Some(Duration::from_secs(60)));
        assert!(!t.should_stop());
        t.cancel();
        assert!(t.should_stop());
        assert!(t.check().unwrap_err().to_string().contains("cancelled"));
        // Re-arming for the next statement clears it.
        t.arm(Some(Duration::from_secs(60)));
        assert!(!t.should_stop());
    }

    #[test]
    fn check_samples_at_the_interval() {
        let t = Arc::new(QueryCancel::new());
        t.arm(Some(Duration::from_millis(1)));
        std::thread::sleep(Duration::from_millis(15));
        let mut c = CancelCheck::new(t);
        // The first CHECK_INTERVAL-1 ticks are free (no clock read, no error).
        for i in 1..CHECK_INTERVAL {
            assert!(c.tick().is_ok(), "tick {i} should not consult the token");
        }
        // The interval boundary observes the expired deadline.
        assert!(
            c.tick().is_err(),
            "tick at the interval must stop the query"
        );
    }
}
