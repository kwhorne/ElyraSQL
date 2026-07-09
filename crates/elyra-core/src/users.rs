//! Persistent user accounts and privilege mapping.
//!
//! User accounts live in the single database file under the `sys::user::`
//! keyspace, so `CREATE USER` / `GRANT` survive restarts and are the one source
//! of truth shared by the SQL engine (which writes them) and the server's
//! authenticator (which reads them). Passwords are stored only as
//! `SHA1(SHA1(password))` — never in clear text.

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::Privilege;

/// Key prefix for persistent user records.
pub const USER_PREFIX: &[u8] = b"sys::user::";

/// A stored account: password digest plus a global privilege level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    /// `SHA1(SHA1(password))` — the same digest MySQL keeps.
    pub digest: [u8; 20],
    /// Coarse global privilege (Read / Write / Admin).
    pub privilege: Privilege,
}

/// Storage key for a user record.
pub fn user_key(name: &str) -> Vec<u8> {
    let mut k = USER_PREFIX.to_vec();
    k.extend_from_slice(name.as_bytes());
    k
}

/// `SHA1(SHA1(password))`.
pub fn password_digest(password: &[u8]) -> [u8; 20] {
    let mut out = [0u8; 20];
    out.copy_from_slice(&Sha1::digest(Sha1::digest(password)));
    out
}

/// Serialize a record for storage.
pub fn encode_user(rec: &UserRecord) -> Vec<u8> {
    bincode::serialize(rec).expect("UserRecord serializes")
}

/// Deserialize a stored record.
pub fn decode_user(bytes: &[u8]) -> Option<UserRecord> {
    bincode::deserialize(bytes).ok()
}

/// Map a set of granted SQL action names (upper-cased, e.g. `SELECT`,
/// `INSERT`, `ALL`) to a coarse privilege level. `ALL`/`GRANT OPTION`/`SUPER`
/// grant Admin; any data- or schema-modifying action grants at least Write;
/// otherwise Read.
pub fn privilege_from_actions<I, S>(actions: I) -> Privilege
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut level = Privilege::Read;
    for a in actions {
        let a = a.as_ref().to_ascii_uppercase();
        let this = match a.as_str() {
            "ALL" | "ALL PRIVILEGES" | "GRANT" | "GRANT OPTION" | "SUPER" | "ADMIN" => {
                Privilege::Admin
            }
            "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "DROP" | "ALTER" | "INDEX" | "TRUNCATE"
            | "REFERENCES" | "WRITE" => Privilege::Write,
            _ => Privilege::Read, // SELECT, USAGE, CONNECT, ...
        };
        if this > level {
            level = this;
        }
    }
    level
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actions_map_to_levels() {
        assert_eq!(privilege_from_actions(["SELECT"]), Privilege::Read);
        assert_eq!(privilege_from_actions(["USAGE"]), Privilege::Read);
        assert_eq!(
            privilege_from_actions(["SELECT", "INSERT"]),
            Privilege::Write
        );
        assert_eq!(privilege_from_actions(["CREATE"]), Privilege::Write);
        assert_eq!(privilege_from_actions(["ALL"]), Privilege::Admin);
        assert_eq!(
            privilege_from_actions(["SELECT", "GRANT OPTION"]),
            Privilege::Admin
        );
    }

    #[test]
    fn user_record_roundtrips() {
        let rec = UserRecord {
            digest: password_digest(b"hunter2"),
            privilege: Privilege::Write,
        };
        let back = decode_user(&encode_user(&rec)).unwrap();
        assert_eq!(back.digest, rec.digest);
        assert_eq!(back.privilege, Privilege::Write);
        assert_ne!(rec.digest, password_digest(b"other"));
    }
}
