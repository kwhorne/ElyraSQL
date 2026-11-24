//! Ports for the servers the tests start.
//!
//! A port the OS hands out for `127.0.0.1:0` is free only until its listener
//! is dropped. Until the server binds it, any outgoing connection -- a test's
//! client, a Raft RPC -- can be given it as its own ephemeral port, and the
//! server then fails to start with "Address already in use". These ports come
//! from below the ephemeral range (Linux starts it at 32768, macOS at 49152),
//! where no outgoing connection is given one, and each is checked when chosen.

use std::collections::HashSet;
use std::hash::{BuildHasher, Hasher};
use std::sync::Mutex;

const LOWEST: u16 = 20_000;
const BELOW: u16 = 32_768;

/// Ports this process has handed out: it is not given one twice.
static GIVEN: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

pub fn free_port() -> u16 {
    let mut given = GIVEN.lock().unwrap();
    let given = given.get_or_insert_with(HashSet::new);
    for _ in 0..10_000 {
        // Each `RandomState` is keyed differently: a random pick per call,
        // and a different sequence in every test process.
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u32(std::process::id());
        let port = LOWEST + (h.finish() % u64::from(BELOW - LOWEST)) as u16;
        if given.contains(&port) {
            continue;
        }
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            given.insert(port);
            return port;
        }
    }
    panic!("no free port between {LOWEST} and {BELOW}");
}
