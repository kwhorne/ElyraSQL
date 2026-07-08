//! Authentication for the MySQL protocol (`mysql_native_password`).
//!
//! Passwords are never stored in plaintext: ElyraSQL keeps
//! `SHA1(SHA1(password))` (the same digest MySQL stores in
//! `authentication_string`) and verifies the challenge/response without ever
//! reconstructing the password.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use sha1::{Digest, Sha1};

/// Credential store. When empty (`open`), all logins are accepted — intended
/// only for local development, and logged loudly by the server.
pub struct Auth {
    users: HashMap<String, [u8; 20]>,
    open: bool,
}

impl Auth {
    /// Open mode: accept any user (dev only).
    pub fn open() -> Self {
        Auth { users: HashMap::new(), open: true }
    }

    /// Require exactly the given user/password.
    pub fn single(user: &str, password: &str) -> Self {
        let mut users = HashMap::new();
        users.insert(user.to_string(), double_sha1(password.as_bytes()));
        Auth { users, open: false }
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Verify a `mysql_native_password` handshake response.
    ///
    /// The client sends `token = SHA1(pw) XOR SHA1(salt || SHA1(SHA1(pw)))`.
    /// We recover `SHA1(pw)` and confirm `SHA1(SHA1(pw))` matches the stored
    /// digest.
    pub fn verify(&self, username: &[u8], salt: &[u8], auth_data: &[u8]) -> bool {
        if self.open {
            return true;
        }
        let Ok(user) = std::str::from_utf8(username) else { return false };
        let Some(stored) = self.users.get(user) else { return false };

        if auth_data.is_empty() {
            // Empty password path.
            return *stored == double_sha1(b"");
        }
        if auth_data.len() != 20 {
            return false;
        }

        let mut h = Sha1::new();
        h.update(salt);
        h.update(stored);
        let token = h.finalize();

        let mut stage1 = [0u8; 20];
        for i in 0..20 {
            stage1[i] = auth_data[i] ^ token[i];
        }
        sha1(&stage1) == *stored
    }
}

fn sha1(bytes: &[u8]) -> [u8; 20] {
    let d = Sha1::digest(bytes);
    let mut out = [0u8; 20];
    out.copy_from_slice(&d);
    out
}

fn double_sha1(pw: &[u8]) -> [u8; 20] {
    sha1(&sha1(pw))
}

/// Generate a fresh per-connection salt of printable, non-`\0`/`$` bytes.
///
/// Note: this is a fast, non-cryptographic PRNG seeded from the clock and a
/// global counter. It is unique per connection; a CSPRNG (getrandom) is the
/// recommended hardening for production.
pub fn generate_salt() -> [u8; 20] {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
    let mut x = nanos ^ (COUNTER.fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed).wrapping_mul(0xD1B54A32D192ED03));
    let mut salt = [0u8; 20];
    for b in salt.iter_mut() {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        let v = (x.wrapping_mul(0x2545F4914F6CDD1D) >> 33) as u32;
        // Map into printable ASCII range 0x21..=0x7e, avoiding '\0' and '$'.
        let mut c = 0x21 + (v % (0x7e - 0x21));
        if c == b'$' as u32 {
            c += 1;
        }
        *b = c as u8;
    }
    salt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_mode_accepts_all() {
        assert!(Auth::open().verify(b"anyone", b"salt................", b""));
    }

    #[test]
    fn native_password_roundtrip() {
        // Reproduce what a client computes, then verify it.
        let salt = generate_salt();
        let stored = double_sha1(b"s3cret");
        let stage1 = sha1(b"s3cret");
        let mut h = Sha1::new();
        h.update(salt);
        h.update(stored);
        let token = h.finalize();
        let mut resp = [0u8; 20];
        for i in 0..20 {
            resp[i] = stage1[i] ^ token[i];
        }
        let auth = Auth::single("root", "s3cret");
        assert!(auth.verify(b"root", &salt, &resp));
        assert!(!auth.verify(b"root", &salt, &[0u8; 20]));
        assert!(!auth.verify(b"nobody", &salt, &resp));
    }

    #[test]
    fn salts_differ() {
        assert_ne!(generate_salt(), generate_salt());
    }
}
