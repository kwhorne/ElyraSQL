//! The process environment, as tests in this crate share it.
//!
//! The cluster secret and the open-auth opt-in are read from env vars on every
//! connection, and the test threads of one binary share one environment. A
//! test that sets one, and a test that dials a peer while it is set, each hold
//! `ENV_LOCK`: a Raft test dialling a fake follower used to wait out its
//! deadline for a handshake nonce while a replication test had a secret set.

/// Held across `.await`, so an async mutex.
pub(crate) static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(crate) fn set_env(key: &str, value: Option<&str>) {
    match value {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
}
