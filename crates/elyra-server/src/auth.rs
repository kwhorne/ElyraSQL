//! Authentication for the MySQL protocol: `mysql_native_password` (default) and
//! `caching_sha2_password` (MySQL 8's default plugin, opt-in via
//! `ELYRASQL_AUTH_PLUGIN`).
//!
//! Passwords are never stored in plaintext: ElyraSQL keeps
//! `SHA1(SHA1(password))` (the same digest MySQL stores in
//! `authentication_string`). native_password verifies the challenge/response
//! against it; caching_sha2 obtains the cleartext (over TLS, or RSA-decrypted on
//! a plaintext connection) and checks `SHA1(SHA1(cleartext))` against the stored
//! digest -- the password is never persisted in the clear.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use elyra_core::users::{decode_user, user_key, UserRecord, USER_PREFIX};
use elyra_core::Privilege;
use elyra_storage::Db;
use sha1::{Digest, Sha1};
use tracing::warn;

/// Failed-login tracking for one account (brute-force lockout).
#[derive(Default)]
struct FailState {
    fails: u32,
    locked_until: Option<Instant>,
}

/// Max consecutive failed logins before an account is temporarily locked
/// (`ELYRASQL_AUTH_MAX_FAILURES`, 0 = disabled).
fn max_failures() -> u32 {
    std::env::var("ELYRASQL_AUTH_MAX_FAILURES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

/// Lockout duration in seconds after too many failures
/// (`ELYRASQL_AUTH_LOCKOUT_SECS`).
fn lockout_secs() -> u64 {
    std::env::var("ELYRASQL_AUTH_LOCKOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60)
}

/// Credential store. Accounts come from two places: a `bootstrap` map supplied
/// at startup (the CLI `--user` / `--auth` flags, always valid) and persistent
/// accounts stored in the database under `sys::user::` (created with
/// `CREATE USER` / `GRANT`). When neither exists, all logins are accepted —
/// intended only for local development, and logged loudly by the server.
pub struct Auth {
    bootstrap: HashMap<String, ([u8; 20], Privilege)>,
    db: Option<Db>,
    /// Per-account failed-login state for brute-force lockout.
    failures: Mutex<HashMap<String, FailState>>,
    /// RSA keypair for `caching_sha2_password` full auth over a non-TLS channel
    /// (the client encrypts the password with this key). Generated on first use.
    rsa: std::sync::OnceLock<RsaKey>,
}

struct RsaKey {
    private: rsa::RsaPrivateKey,
    public_pem: String,
}

fn generate_rsa() -> RsaKey {
    use rsa::pkcs8::EncodePublicKey;
    let mut rng = rand::thread_rng();
    let private = rsa::RsaPrivateKey::new(&mut rng, 2048).expect("RSA keygen");
    let public = rsa::RsaPublicKey::from(&private);
    let public_pem = public
        .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
        .expect("RSA public PEM");
    RsaKey {
        private,
        public_pem,
    }
}

impl Auth {
    /// Open mode: no bootstrap accounts. Accepts all logins as Admin until a
    /// persistent account exists.
    pub fn open() -> Self {
        Auth {
            bootstrap: HashMap::new(),
            db: None,
            failures: Mutex::new(HashMap::new()),
            rsa: std::sync::OnceLock::new(),
        }
    }

    /// Require exactly the given user/password, with Admin privileges.
    pub fn single(user: &str, password: &str) -> Self {
        Self::with_users(vec![(
            user.to_string(),
            password.to_string(),
            Privilege::Admin,
        )])
    }

    /// Build from explicit `(user, password, privilege)` triples.
    pub fn with_users(entries: Vec<(String, String, Privilege)>) -> Self {
        let mut bootstrap = HashMap::new();
        for (u, p, priv_) in entries {
            bootstrap.insert(u, (double_sha1(p.as_bytes()), priv_));
        }
        Auth {
            bootstrap,
            db: None,
            failures: Mutex::new(HashMap::new()),
            rsa: std::sync::OnceLock::new(),
        }
    }

    /// Attach the persistent user store (the live database) so `CREATE USER` /
    /// `GRANT` accounts are honoured.
    pub fn with_db(mut self, db: Db) -> Self {
        self.db = Some(db);
        self
    }

    /// A persistent account, if one exists for `user`.
    fn persistent(&self, user: &str) -> Option<UserRecord> {
        let db = self.db.as_ref()?;
        let snap = db.snapshot().ok()?;
        let bytes = snap.get(&user_key(user)).ok()??;
        decode_user(&bytes)
    }

    /// Whether any persistent account exists (turns off dev open mode).
    fn any_persistent(&self) -> bool {
        let Some(db) = &self.db else {
            return false;
        };
        let Ok(snap) = db.snapshot() else {
            return false;
        };
        snap.scan_range(USER_PREFIX, None, 1)
            .map(|rows| rows.iter().any(|(k, _)| k.starts_with(USER_PREFIX)))
            .unwrap_or(false)
    }

    /// Look up an account's stored digest and privilege from either source.
    fn lookup(&self, user: &str) -> Option<([u8; 20], Privilege)> {
        if let Some((d, p)) = self.bootstrap.get(user) {
            return Some((*d, *p));
        }
        self.persistent(user).map(|r| (r.digest, r.privilege))
    }

    /// True when authentication is disabled (no accounts at all).
    pub fn is_open(&self) -> bool {
        self.bootstrap.is_empty() && !self.any_persistent()
    }

    /// Privilege granted to `username` (Admin in open mode; Read if unknown).
    pub fn privilege(&self, username: &[u8]) -> Privilege {
        if self.is_open() {
            return Privilege::Admin;
        }
        std::str::from_utf8(username)
            .ok()
            .and_then(|u| self.lookup(u))
            .map(|(_, p)| p)
            .unwrap_or(Privilege::Read)
    }

    /// Verify a `mysql_native_password` handshake response.
    ///
    /// The client sends `token = SHA1(pw) XOR SHA1(salt || SHA1(SHA1(pw)))`.
    /// We recover `SHA1(pw)` and confirm `SHA1(SHA1(pw))` matches the stored
    /// digest.
    pub fn verify(&self, username: &[u8], salt: &[u8], auth_data: &[u8]) -> bool {
        if self.is_open() {
            return true;
        }
        let Ok(user) = std::str::from_utf8(username) else {
            return false;
        };

        // Brute-force lockout: reject while an account is temporarily locked.
        let threshold = max_failures();
        if threshold > 0 {
            let locked = self
                .failures
                .lock()
                .unwrap()
                .get(user)
                .and_then(|f| f.locked_until)
                .is_some_and(|t| Instant::now() < t);
            if locked {
                warn!(user, "authentication rejected: account temporarily locked");
                return false;
            }
        }

        let ok = self.verify_raw(user, salt, auth_data);

        if threshold > 0 {
            let mut map = self.failures.lock().unwrap();
            if ok {
                map.remove(user);
            } else {
                let f = map.entry(user.to_string()).or_default();
                f.fails += 1;
                if f.fails >= threshold {
                    f.locked_until =
                        Some(Instant::now() + std::time::Duration::from_secs(lockout_secs()));
                    f.fails = 0;
                    warn!(
                        user,
                        lockout_secs = lockout_secs(),
                        "account locked after too many failed logins"
                    );
                } else {
                    warn!(user, fails = f.fails, "failed login");
                }
            }
        }
        ok
    }

    /// The raw `mysql_native_password` challenge/response check.
    fn verify_raw(&self, user: &str, salt: &[u8], auth_data: &[u8]) -> bool {
        let Some((stored, _)) = self.lookup(user) else {
            return false;
        };
        let stored = &stored;

        if auth_data.is_empty() {
            // Empty password path.
            return ct_eq(stored, &double_sha1(b""));
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
        ct_eq(&sha1(&stage1), stored)
    }

    /// Whether an account needs a password (so `caching_sha2_password` must run
    /// full authentication). Open mode and empty-password accounts do not.
    pub fn requires_password(&self, username: &[u8]) -> bool {
        if self.is_open() {
            return false;
        }
        match std::str::from_utf8(username).ok().and_then(|u| self.lookup(u)) {
            Some((stored, _)) => !ct_eq(&stored, &double_sha1(b"")),
            None => true, // unknown user: force full auth (which then fails)
        }
    }

    /// Verify a cleartext password (the `caching_sha2_password` full-auth path),
    /// with the same brute-force lockout accounting as [`verify`].
    pub fn verify_cleartext(&self, username: &[u8], password: &[u8]) -> bool {
        if self.is_open() {
            return true;
        }
        let Ok(user) = std::str::from_utf8(username) else {
            return false;
        };
        let threshold = max_failures();
        if threshold > 0 {
            let locked = self
                .failures
                .lock()
                .unwrap()
                .get(user)
                .and_then(|f| f.locked_until)
                .is_some_and(|t| Instant::now() < t);
            if locked {
                warn!(user, "authentication rejected: account temporarily locked");
                return false;
            }
        }
        let ok = match self.lookup(user) {
            Some((stored, _)) => ct_eq(&double_sha1(password), &stored),
            None => false,
        };
        if threshold > 0 {
            let mut map = self.failures.lock().unwrap();
            if ok {
                map.remove(user);
            } else {
                let f = map.entry(user.to_string()).or_default();
                f.fails += 1;
                if f.fails >= threshold {
                    f.locked_until =
                        Some(Instant::now() + std::time::Duration::from_secs(lockout_secs()));
                    f.fails = 0;
                    warn!(user, "account locked after too many failed logins");
                } else {
                    warn!(user, fails = f.fails, "failed login");
                }
            }
        }
        ok
    }

    /// PEM (SPKI) of the RSA public key clients use to encrypt the password for
    /// `caching_sha2_password` full auth over a non-TLS connection.
    pub fn caching_sha2_public_key_pem(&self) -> String {
        self.rsa.get_or_init(generate_rsa).public_pem.clone()
    }

    /// Decrypt an RSA-OAEP(SHA-1) ciphertext from a `caching_sha2_password`
    /// full-auth exchange (returns `password XOR nonce`, still to be un-XORed).
    pub fn caching_sha2_decrypt(&self, ciphertext: &[u8]) -> Option<Vec<u8>> {
        let key = self.rsa.get_or_init(generate_rsa);
        let padding = rsa::Oaep::new::<sha1::Sha1>();
        key.private.decrypt(padding, ciphertext).ok()
    }
}

/// Constant-time byte comparison (no early exit) to avoid password-hash timing
/// attacks.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
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

/// Generate a fresh per-connection salt of printable, non-`\0`/`$` bytes,
/// using the OS cryptographically-secure RNG (`getrandom`). Falls back to a
/// clock+counter PRNG only if the OS RNG is unavailable.
pub fn generate_salt() -> [u8; 20] {
    let mut raw = [0u8; 20];
    if getrandom::getrandom(&mut raw).is_err() {
        // Extremely rare fallback: seed a weak PRNG from the clock + a counter.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut x = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
            ^ COUNTER
                .fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed)
                .wrapping_mul(0xD1B54A32D192ED03);
        for b in raw.iter_mut() {
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            *b = (x.wrapping_mul(0x2545F4914F6CDD1D) >> 56) as u8;
        }
    }
    let mut salt = [0u8; 20];
    for (b, r) in salt.iter_mut().zip(raw) {
        // Map into printable ASCII 0x21..=0x7e, avoiding '\0' and '$'.
        let mut c = 0x21 + (r as u32 % (0x7e - 0x21));
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
