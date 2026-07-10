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

/// Key prefix for per-table grants of one user.
pub fn table_grant_prefix(user: &str) -> Vec<u8> {
    format!("sys::tgrant::{user}::").into_bytes()
}

/// Key prefix for the roles granted to one user.
pub fn role_member_prefix(user: &str) -> Vec<u8> {
    format!("sys::rolemember::{user}::").into_bytes()
}

/// Key prefix for a user's per-column SELECT grants on one table.
pub fn col_grant_prefix(user: &str, table: &str) -> Vec<u8> {
    format!("sys::colgrant::{user}::{}::", table.to_ascii_lowercase()).into_bytes()
}

/// Storage key for a user's SELECT grant on `table.column`.
pub fn col_grant_key(user: &str, table: &str, column: &str) -> Vec<u8> {
    let mut k = col_grant_prefix(user, table);
    k.extend_from_slice(column.to_ascii_lowercase().as_bytes());
    k
}

/// Storage key for "`user` is a member of `role`".
pub fn role_member_key(user: &str, role: &str) -> Vec<u8> {
    let mut k = role_member_prefix(user);
    k.extend_from_slice(role.as_bytes());
    k
}

/// Marker key prefix distinguishing a principal that is a role (vs a login user).
pub fn role_flag_key(role: &str) -> Vec<u8> {
    format!("sys::role::{role}").into_bytes()
}

/// Storage key for a user's global privilege flag set (`sys::ugrant::<user>`).
/// Holds the fine-grained set of granted privileges so `REVOKE` removes only
/// the named privileges instead of collapsing the account to a coarse tier.
pub fn ugrant_key(user: &str) -> Vec<u8> {
    format!("sys::ugrant::{user}").into_bytes()
}

/// Individual privilege flags. Stored as a bitset so GRANT/REVOKE are set
/// union/difference; the coarse [`Privilege`] tier (what enforcement checks) is
/// derived from the set with [`tier_from_privset`].
pub mod priv_bits {
    pub const SELECT: u32 = 1 << 0;
    pub const INSERT: u32 = 1 << 1;
    pub const UPDATE: u32 = 1 << 2;
    pub const DELETE: u32 = 1 << 3;
    pub const CREATE: u32 = 1 << 4;
    pub const DROP: u32 = 1 << 5;
    pub const ALTER: u32 = 1 << 6;
    pub const INDEX: u32 = 1 << 7;
    pub const TRUNCATE: u32 = 1 << 8;
    pub const REFERENCES: u32 = 1 << 9;
    /// GRANT OPTION / SUPER / ADMIN — the admin marker.
    pub const GRANT_OPTION: u32 = 1 << 10;

    /// Data- and schema-modifying privileges (map to the `Write` tier).
    pub const WRITE: u32 =
        INSERT | UPDATE | DELETE | CREATE | DROP | ALTER | INDEX | TRUNCATE | REFERENCES;
    /// Every privilege (what `ALL PRIVILEGES` grants).
    pub const ALL: u32 = SELECT | WRITE | GRANT_OPTION;
}

/// Map granted SQL action names to a privilege flag set (parallel to
/// [`privilege_from_actions`], but preserving the individual privileges).
pub fn privset_from_actions<I, S>(actions: I) -> u32
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    use priv_bits::*;
    let mut bits = 0u32;
    for a in actions {
        bits |= match a.as_ref().to_ascii_uppercase().as_str() {
            "ALL" | "ALL PRIVILEGES" => ALL,
            "GRANT" | "GRANT OPTION" | "SUPER" | "ADMIN" => GRANT_OPTION,
            "SELECT" => SELECT,
            "INSERT" => INSERT,
            "UPDATE" => UPDATE,
            "DELETE" => DELETE,
            "CREATE" => CREATE,
            "DROP" => DROP,
            "ALTER" => ALTER,
            "INDEX" => INDEX,
            "TRUNCATE" => TRUNCATE,
            "REFERENCES" => REFERENCES,
            "WRITE" => WRITE,
            _ => 0, // SELECT-level: USAGE, CONNECT, ...
        };
    }
    bits
}

/// Derive the coarse enforcement tier from a privilege flag set. Consistent
/// with [`privilege_from_actions`]: any admin marker => Admin, any write
/// privilege => Write, else Read.
pub fn tier_from_privset(bits: u32) -> Privilege {
    if bits & priv_bits::GRANT_OPTION != 0 {
        Privilege::Admin
    } else if bits & priv_bits::WRITE != 0 {
        Privilege::Write
    } else {
        Privilege::Read
    }
}

/// Expand a coarse tier into a flag set (used to migrate accounts created
/// before per-privilege tracking, which only stored a tier).
pub fn privset_from_tier(p: Privilege) -> u32 {
    match p {
        Privilege::Read => priv_bits::SELECT,
        Privilege::Write => priv_bits::SELECT | priv_bits::WRITE,
        Privilege::Admin => priv_bits::ALL,
    }
}

/// Encode a privilege flag set (4-byte little-endian).
pub fn encode_privset(bits: u32) -> Vec<u8> {
    bits.to_le_bytes().to_vec()
}

/// Decode a privilege flag set; malformed input decodes as empty.
pub fn decode_privset(bytes: &[u8]) -> u32 {
    match bytes.try_into() {
        Ok(b) => u32::from_le_bytes(b),
        Err(_) => 0,
    }
}

/// Human-readable, comma-separated privilege list for a flag set (for
/// `SHOW GRANTS`). Returns `"ALL PRIVILEGES"` when every privilege is set,
/// `"USAGE"` when none.
pub fn privset_to_names(bits: u32) -> String {
    use priv_bits::*;
    if bits & ALL == ALL {
        return "ALL PRIVILEGES".into();
    }
    let mut parts = Vec::new();
    for (bit, name) in [
        (SELECT, "SELECT"),
        (INSERT, "INSERT"),
        (UPDATE, "UPDATE"),
        (DELETE, "DELETE"),
        (CREATE, "CREATE"),
        (DROP, "DROP"),
        (ALTER, "ALTER"),
        (INDEX, "INDEX"),
        (TRUNCATE, "TRUNCATE"),
        (REFERENCES, "REFERENCES"),
        (GRANT_OPTION, "GRANT OPTION"),
    ] {
        if bits & bit != 0 {
            parts.push(name);
        }
    }
    if parts.is_empty() {
        "USAGE".into()
    } else {
        parts.join(", ")
    }
}

/// Storage key for a user's grant on a specific table.
pub fn table_grant_key(user: &str, table: &str) -> Vec<u8> {
    let mut k = table_grant_prefix(user);
    k.extend_from_slice(table.to_ascii_lowercase().as_bytes());
    k
}

/// Encode a privilege level as a single byte.
pub fn encode_privilege(p: Privilege) -> Vec<u8> {
    vec![match p {
        Privilege::Read => 0,
        Privilege::Write => 1,
        Privilege::Admin => 2,
    }]
}

/// Decode a privilege level from a single byte.
pub fn decode_privilege(bytes: &[u8]) -> Option<Privilege> {
    match bytes.first()? {
        0 => Some(Privilege::Read),
        1 => Some(Privilege::Write),
        2 => Some(Privilege::Admin),
        _ => None,
    }
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
    fn revoke_removes_only_named_privilege() {
        use priv_bits::*;
        // An admin (ALL) who has INSERT revoked keeps admin — the old model
        // collapsed the whole account to Read here.
        let admin = privset_from_actions(["ALL"]);
        let after = admin & !privset_from_actions(["INSERT"]);
        assert_eq!(tier_from_privset(after), Privilege::Admin);
        assert_eq!(after & INSERT, 0);

        // A user with INSERT+UPDATE who loses INSERT still has UPDATE (Write),
        // rather than being reset to Read.
        let w = privset_from_actions(["INSERT", "UPDATE"]);
        let after = w & !privset_from_actions(["INSERT"]);
        assert_eq!(tier_from_privset(after), Privilege::Write);
        assert_eq!(after & UPDATE, UPDATE);
        assert_eq!(after & INSERT, 0);

        // Revoking the last write privilege drops to Read.
        let after = after & !privset_from_actions(["UPDATE"]);
        assert_eq!(tier_from_privset(after), Privilege::Read);

        // Legacy migration + encode round-trip.
        assert_eq!(
            tier_from_privset(privset_from_tier(Privilege::Admin)),
            Privilege::Admin
        );
        assert_eq!(decode_privset(&encode_privset(admin)), admin);
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
