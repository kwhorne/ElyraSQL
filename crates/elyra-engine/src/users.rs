//! User management DDL: `CREATE USER`, `DROP USER`, `ALTER USER` /
//! `SET PASSWORD`, `GRANT`, `REVOKE`, and `SHOW GRANTS`.
//!
//! MySQL's user grammar isn't parsed by the SQL frontend, and privileges in
//! ElyraSQL are coarse (global Read / Write / Admin), so we parse these
//! statements here with a small tokenizer and store accounts in the database
//! under `sys::user::` — the same records the authenticator reads.

use elyra_core::users::{
    decode_privilege, decode_user, encode_privilege, encode_user, password_digest,
    privilege_from_actions, role_flag_key, role_member_key, role_member_prefix, table_grant_key,
    table_grant_prefix, user_key, UserRecord, USER_PREFIX,
};
use elyra_core::{Error, Privilege, Result, Schema, Value};

use crate::session::Session;
use crate::stream::RowStream;
use crate::QueryResult;

/// A single lexical token: a bare word/number, a quoted string, or a symbol.
#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(String),
    Str(String),
    Sym(char),
}

impl Tok {
    /// The textual value of a word or quoted string (symbols yield "").
    fn text(&self) -> &str {
        match self {
            Tok::Word(s) | Tok::Str(s) => s,
            Tok::Sym(_) => "",
        }
    }
    fn is_word(&self, kw: &str) -> bool {
        matches!(self, Tok::Word(s) if s.eq_ignore_ascii_case(kw))
    }
}

fn tokenize(sql: &str) -> Vec<Tok> {
    let mut out = Vec::new();
    let cs: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '\'' || c == '"' || c == '`' {
            let quote = c;
            i += 1;
            let mut s = String::new();
            while i < cs.len() {
                if cs[i] == quote {
                    // Doubled quote → literal quote.
                    if i + 1 < cs.len() && cs[i + 1] == quote {
                        s.push(quote);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(cs[i]);
                i += 1;
            }
            out.push(Tok::Str(s));
        } else if c == ',' || c == '@' || c == '=' || c == '(' || c == ')' || c == '*' || c == ';' {
            out.push(Tok::Sym(c));
            i += 1;
        } else if c.is_alphanumeric() || c == '_' || c == '.' || c == '$' || c == '%' || c == '-' {
            let mut s = String::new();
            while i < cs.len()
                && (cs[i].is_alphanumeric()
                    || cs[i] == '_'
                    || cs[i] == '.'
                    || cs[i] == '$'
                    || cs[i] == '%'
                    || cs[i] == '-')
            {
                s.push(cs[i]);
                i += 1;
            }
            out.push(Tok::Word(s));
        } else {
            i += 1; // skip anything else
        }
    }
    out
}

/// Read a user specification (`name` or `'name'@'host'`) starting at `i`,
/// returning the account name and the index after it. The host part is parsed
/// and ignored (ElyraSQL accounts are host-independent).
fn parse_userspec(toks: &[Tok], mut i: usize) -> Option<(String, usize)> {
    let name = match toks.get(i)? {
        Tok::Word(s) | Tok::Str(s) => s.clone(),
        _ => return None,
    };
    i += 1;
    if matches!(toks.get(i), Some(Tok::Sym('@'))) {
        i += 1;
        if matches!(toks.get(i), Some(Tok::Word(_)) | Some(Tok::Str(_))) {
            i += 1;
        }
    }
    Some((name, i))
}

/// Enforce the password strength policy on a new password. Configurable via env:
/// `ELYRASQL_PASSWORD_POLICY=off` disables it; `ELYRASQL_PASSWORD_MIN_LEN`
/// (default 8) sets the minimum length; `ELYRASQL_PASSWORD_REQUIRE_MIXED`
/// (default true) requires at least one letter and one digit.
pub fn check_password_policy(pw: &str) -> Result<()> {
    if std::env::var("ELYRASQL_PASSWORD_POLICY")
        .map(|v| v.eq_ignore_ascii_case("off"))
        .unwrap_or(false)
    {
        return Ok(());
    }
    let min_len: usize = std::env::var("ELYRASQL_PASSWORD_MIN_LEN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    if pw.chars().count() < min_len {
        return Err(Error::Query(format!(
            "password does not meet policy: minimum length is {min_len} \
             (set ELYRASQL_PASSWORD_POLICY=off to disable)"
        )));
    }
    let require_mixed = std::env::var("ELYRASQL_PASSWORD_REQUIRE_MIXED")
        .map(|v| !v.eq_ignore_ascii_case("false") && v != "0")
        .unwrap_or(true);
    if require_mixed {
        let has_letter = pw.chars().any(|c| c.is_alphabetic());
        let has_digit = pw.chars().any(|c| c.is_ascii_digit());
        if !(has_letter && has_digit) {
            return Err(Error::Query(
                "password does not meet policy: must contain both letters and digits \
                 (set ELYRASQL_PASSWORD_REQUIRE_MIXED=false to relax)"
                    .into(),
            ));
        }
    }
    Ok(())
}

/// True if `sql` (already trimmed) begins a user-management statement we handle.
pub fn is_user_stmt(head: &str) -> bool {
    let h = head.trim_start();
    let starts = |kw: &str| h.len() >= kw.len() && h[..kw.len()].eq_ignore_ascii_case(kw);
    starts("create user")
        || starts("drop user")
        || starts("alter user")
        || starts("create role")
        || starts("drop role")
        || starts("set password")
        || starts("grant ")
        || starts("revoke ")
        || starts("show grants")
        || h.eq_ignore_ascii_case("show grants")
}

/// A user's per-column SELECT grants on `table`. `None` means the user is not
/// column-restricted on this table (no column grants); `Some(cols)` restricts
/// reads to exactly those columns.
pub async fn column_grants(sess: &Session, user: &str, table: &str) -> Result<Option<Vec<String>>> {
    if user.is_empty() {
        return Ok(None);
    }
    let prefix = elyra_core::users::col_grant_prefix(user, table);
    let batch = sess.scan_batch(prefix.clone(), None, 4096).await?;
    if batch.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        batch
            .iter()
            .map(|(k, _)| String::from_utf8_lossy(&k[prefix.len()..]).into_owned())
            .collect(),
    ))
}

/// The role names granted (directly) to `user`.
pub async fn roles_of(sess: &Session, user: &str) -> Result<Vec<String>> {
    let prefix = role_member_prefix(user);
    let batch = sess.scan_batch(prefix.clone(), None, 4096).await?;
    Ok(batch
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(&k[prefix.len()..]).into_owned())
        .collect())
}

/// A principal's own global privilege (`Read` if unknown).
async fn own_privilege(sess: &Session, name: &str) -> Result<Privilege> {
    Ok(sess
        .get(user_key(name))
        .await?
        .and_then(|b| decode_user(&b))
        .map(|r| r.privilege)
        .unwrap_or(Privilege::Read))
}

/// Effective global privilege of `user`: their own level raised by every role
/// granted to them.
pub async fn effective_global(sess: &Session, user: &str) -> Result<Privilege> {
    let mut p = own_privilege(sess, user).await?;
    for role in roles_of(sess, user).await? {
        p = p.max(own_privilege(sess, &role).await?);
    }
    Ok(p)
}

/// A principal's own grant on one table (`Read` default).
async fn own_table_grant(sess: &Session, name: &str, table: &str) -> Result<Privilege> {
    Ok(sess
        .get(table_grant_key(name, table))
        .await?
        .and_then(|b| decode_privilege(&b))
        .unwrap_or(Privilege::Read))
}

/// Effective per-table privilege of `user` on `table`, including roles.
pub async fn effective_table_grant(sess: &Session, user: &str, table: &str) -> Result<Privilege> {
    let mut p = own_table_grant(sess, user, table).await?;
    for role in roles_of(sess, user).await? {
        p = p.max(own_table_grant(sess, &role, table).await?);
    }
    Ok(p)
}

pub async fn execute(sql: &str, sess: &Session, privilege: Privilege) -> Result<QueryResult> {
    let toks = tokenize(sql);
    let w = |i: usize| {
        toks.get(i)
            .map(|t| t.text().to_string())
            .unwrap_or_default()
    };

    // SHOW GRANTS [FOR user] — readable; others require Admin.
    if toks.first().map(|t| t.is_word("show")).unwrap_or(false) {
        return show_grants(&toks, sess).await;
    }

    if privilege < Privilege::Admin {
        return Err(Error::Query(
            "access denied: user management requires ADMIN privilege".into(),
        ));
    }

    // CREATE ROLE [IF NOT EXISTS] name [, name ...]
    if toks.first().map(|t| t.is_word("create")).unwrap_or(false)
        && toks.get(1).map(|t| t.is_word("role")).unwrap_or(false)
    {
        let mut i = 2;
        if toks.get(i).map(|t| t.is_word("if")).unwrap_or(false) {
            i += 3; // IF NOT EXISTS
        }
        let mut puts = Vec::new();
        while let Some((name, j)) = parse_userspec(&toks, i) {
            // A role is a principal (empty password) plus a role marker.
            let rec = UserRecord {
                digest: password_digest(b""),
                privilege: Privilege::Read,
            };
            puts.push((user_key(&name), encode_user(&rec)));
            puts.push((role_flag_key(&name), vec![1]));
            i = j;
            if matches!(toks.get(i), Some(Tok::Sym(','))) {
                i += 1;
            } else {
                break;
            }
        }
        if puts.is_empty() {
            return Err(Error::Parse("CREATE ROLE: expected a role name".into()));
        }
        sess.commit_write(puts, vec![]).await?;
        return Ok(QueryResult::Affected(0));
    }

    // DROP ROLE [IF EXISTS] name [, name ...]
    if toks.first().map(|t| t.is_word("drop")).unwrap_or(false)
        && toks.get(1).map(|t| t.is_word("role")).unwrap_or(false)
    {
        let mut i = 2;
        if toks.get(i).map(|t| t.is_word("if")).unwrap_or(false) {
            i += 2;
        }
        let mut deletes = Vec::new();
        while let Some((name, j)) = parse_userspec(&toks, i) {
            deletes.push(user_key(&name));
            deletes.push(role_flag_key(&name));
            i = j;
            if matches!(toks.get(i), Some(Tok::Sym(','))) {
                i += 1;
            } else {
                break;
            }
        }
        if deletes.is_empty() {
            return Err(Error::Parse("DROP ROLE: expected a role name".into()));
        }
        sess.commit_write(vec![], deletes).await?;
        return Ok(QueryResult::Affected(0));
    }

    // Role membership: GRANT <role...> TO <user...>  /  REVOKE <role...> FROM
    // <user...> — distinguished from privilege grants by the absence of ON.
    let has_on = toks.iter().any(|t| t.is_word("on"));
    if !has_on {
        if toks.first().map(|t| t.is_word("grant")).unwrap_or(false) {
            return role_membership(&toks, sess, true).await;
        }
        if toks.first().map(|t| t.is_word("revoke")).unwrap_or(false) {
            return role_membership(&toks, sess, false).await;
        }
    }

    // CREATE USER [IF NOT EXISTS] spec IDENTIFIED BY 'pw'
    if toks.first().map(|t| t.is_word("create")).unwrap_or(false) {
        let mut i = 2; // after CREATE USER
        if toks.get(i).map(|t| t.is_word("if")).unwrap_or(false) {
            i += 3; // IF NOT EXISTS
        }
        let (name, mut j) = parse_userspec(&toks, i)
            .ok_or_else(|| Error::Parse("CREATE USER: expected a user name".into()))?;
        if sess.get(user_key(&name)).await?.is_some() {
            return Err(Error::Query(format!("user '{name}' already exists")));
        }
        // IDENTIFIED BY 'pw' (optional → empty password)
        let mut password = String::new();
        if toks
            .get(j)
            .map(|t| t.is_word("identified"))
            .unwrap_or(false)
        {
            // skip IDENTIFIED [WITH auth_plugin] BY
            while j < toks.len() && !toks[j].is_word("by") {
                j += 1;
            }
            if toks.get(j).map(|t| t.is_word("by")).unwrap_or(false) {
                password = w(j + 1);
            }
        }
        check_password_policy(&password)?;
        let rec = UserRecord {
            digest: password_digest(password.as_bytes()),
            privilege: Privilege::Read,
        };
        sess.commit_write(vec![(user_key(&name), encode_user(&rec))], vec![])
            .await?;
        return Ok(QueryResult::Affected(0));
    }

    // DROP USER [IF EXISTS] spec [, spec ...]
    if toks.first().map(|t| t.is_word("drop")).unwrap_or(false) {
        let mut i = 2;
        if toks.get(i).map(|t| t.is_word("if")).unwrap_or(false) {
            i += 2; // IF EXISTS
        }
        let mut deletes = Vec::new();
        while let Some((name, j)) = parse_userspec(&toks, i) {
            if sess.get(user_key(&name)).await?.is_none() {
                return Err(Error::Query(format!("user '{name}' does not exist")));
            }
            deletes.push(user_key(&name));
            deletes.push(elyra_core::users::ugrant_key(&name));
            // Purge every per-user grant so a later CREATE USER with the same
            // name cannot inherit stale privileges.
            for pfx in [
                table_grant_prefix(&name),
                format!("sys::colgrant::{name}::").into_bytes(),
                role_member_prefix(&name),
            ] {
                let mut after: Option<Vec<u8>> = None;
                loop {
                    let batch = sess.scan_batch(pfx.clone(), after.clone(), 512).await?;
                    if batch.is_empty() {
                        break;
                    }
                    for (k, _) in &batch {
                        deletes.push(k.clone());
                    }
                    after = batch.last().map(|(k, _)| k.clone());
                    if batch.len() < 512 {
                        break;
                    }
                }
            }
            i = j;
            if matches!(toks.get(i), Some(Tok::Sym(','))) {
                i += 1;
            } else {
                break;
            }
        }
        if deletes.is_empty() {
            return Err(Error::Parse("DROP USER: expected a user name".into()));
        }
        sess.commit_write(vec![], deletes).await?;
        return Ok(QueryResult::Affected(0));
    }

    // ALTER USER spec IDENTIFIED BY 'pw'  |  SET PASSWORD [FOR spec] = 'pw'
    if toks.first().map(|t| t.is_word("alter")).unwrap_or(false)
        || toks.first().map(|t| t.is_word("set")).unwrap_or(false)
    {
        return set_password(&toks, sess, &w).await;
    }

    // GRANT / REVOKE
    if toks.first().map(|t| t.is_word("grant")).unwrap_or(false) {
        return grant_revoke(&toks, sess, true).await;
    }
    if toks.first().map(|t| t.is_word("revoke")).unwrap_or(false) {
        return grant_revoke(&toks, sess, false).await;
    }

    Err(Error::Parse("unsupported user-management statement".into()))
}

async fn set_password(
    toks: &[Tok],
    sess: &Session,
    w: &impl Fn(usize) -> String,
) -> Result<QueryResult> {
    // Locate the target user and the new password.
    let (name, pw) = if toks[0].is_word("alter") {
        let (name, mut j) = parse_userspec(toks, 2)
            .ok_or_else(|| Error::Parse("ALTER USER: expected a user name".into()))?;
        while j < toks.len() && !toks[j].is_word("by") {
            j += 1;
        }
        (name, w(j + 1))
    } else {
        // SET PASSWORD [FOR spec] = 'pw'
        let mut i = 2; // after SET PASSWORD
        let name = if toks.get(i).map(|t| t.is_word("for")).unwrap_or(false) {
            let (n, j) = parse_userspec(toks, i + 1)
                .ok_or_else(|| Error::Parse("SET PASSWORD FOR: expected a user name".into()))?;
            i = j;
            n
        } else {
            return Err(Error::Query(
                "SET PASSWORD without FOR is not supported (specify FOR '<user>')".into(),
            ));
        };
        // expect = 'pw'   (PASSWORD('pw') form: the quoted arg is the value)
        while i < toks.len() && !matches!(toks[i], Tok::Sym('=')) {
            i += 1;
        }
        // find the first string token after '='
        let mut pw = String::new();
        for t in toks.iter().skip(i + 1) {
            if let Tok::Str(s) = t {
                pw = s.clone();
                break;
            }
        }
        (name, pw)
    };

    let Some(bytes) = sess.get(user_key(&name)).await? else {
        return Err(Error::Query(format!("user '{name}' does not exist")));
    };
    let mut rec =
        decode_user(&bytes).ok_or_else(|| Error::Storage("corrupt user record".into()))?;
    check_password_policy(&pw)?;
    rec.digest = password_digest(pw.as_bytes());
    sess.commit_write(vec![(user_key(&name), encode_user(&rec))], vec![])
        .await?;
    Ok(QueryResult::Affected(0))
}

async fn grant_revoke(toks: &[Tok], sess: &Session, grant: bool) -> Result<QueryResult> {
    // Collect action words up to ON, capturing any `ACTION(col, col)` column list
    // (per-column grants).
    let mut i = 1;
    let mut actions: Vec<String> = Vec::new();
    let mut cols: Vec<String> = Vec::new();
    while i < toks.len() && !toks[i].is_word("on") {
        match &toks[i] {
            Tok::Word(s) => actions.push(s.clone()),
            Tok::Sym('(') => {
                i += 1;
                while i < toks.len() && !matches!(toks[i], Tok::Sym(')')) {
                    if let Tok::Word(s) | Tok::Str(s) = &toks[i] {
                        cols.push(s.clone());
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    // Parse the object clause (ON <obj>). `*`/`*.*`/`db.*` → global; otherwise
    // the last dotted component names a specific table (scoped grant).
    let sep = if grant { "to" } else { "from" };
    let mut obj = String::new();
    if i < toks.len() && toks[i].is_word("on") {
        i += 1;
        while i < toks.len() && !toks[i].is_word(sep) {
            match &toks[i] {
                Tok::Word(s) | Tok::Str(s) => obj.push_str(s),
                Tok::Sym(c) => obj.push(*c),
            }
            i += 1;
        }
    }
    let scoped_table: Option<String> = if obj.is_empty() || obj.contains('*') {
        None
    } else {
        obj.rsplit('.').next().map(|s| s.trim().to_string())
    };
    while i < toks.len() && !toks[i].is_word(sep) {
        i += 1;
    }
    if i >= toks.len() {
        return Err(Error::Parse(format!(
            "{} requires {} <user>",
            if grant { "GRANT" } else { "REVOKE" },
            sep.to_uppercase()
        )));
    }
    i += 1; // past TO/FROM

    let level = privilege_from_actions(&actions);

    // Apply to each grantee.
    let mut applied = 0u64;
    while let Some((name, j)) = parse_userspec(toks, i) {
        let Some(bytes) = sess.get(user_key(&name)).await? else {
            return Err(Error::Query(format!(
                "user '{name}' does not exist (CREATE USER first)"
            )));
        };
        if let (Some(table), false) = (&scoped_table, cols.is_empty()) {
            // Per-column grant: SELECT(col, ...) ON table.
            let mut puts = Vec::new();
            let mut dels = Vec::new();
            for col in &cols {
                let key = elyra_core::users::col_grant_key(&name, table, col);
                if grant {
                    puts.push((key, encode_privilege(Privilege::Read)));
                } else {
                    dels.push(key);
                }
            }
            sess.commit_write(puts, dels).await?;
            applied += 1;
            i = j;
            if matches!(toks.get(i), Some(Tok::Sym(','))) {
                i += 1;
                continue;
            } else {
                break;
            }
        }
        if let Some(table) = &scoped_table {
            // Per-table grant: raise (GRANT) or clear (REVOKE) the table level.
            let key = table_grant_key(&name, table);
            if grant {
                sess.commit_write(vec![(key, encode_privilege(level))], vec![])
                    .await?;
            } else {
                sess.commit_write(vec![], vec![key]).await?;
            }
            applied += 1;
            i = j;
            if matches!(toks.get(i), Some(Tok::Sym(','))) {
                i += 1;
                continue;
            } else {
                break;
            }
        }
        let mut rec =
            decode_user(&bytes).ok_or_else(|| Error::Storage("corrupt user record".into()))?;
        // Global privileges are tracked as a flag set, so REVOKE removes only the
        // named privileges (set difference) instead of collapsing the account to
        // Read. The coarse tier stored in the user record is derived from the
        // resulting set for enforcement and backward compatibility.
        let ugkey = elyra_core::users::ugrant_key(&name);
        let current = match sess.get(ugkey.clone()).await? {
            Some(b) => elyra_core::users::decode_privset(&b),
            // Account created before per-privilege tracking: migrate its tier.
            None => elyra_core::users::privset_from_tier(rec.privilege),
        };
        let delta = elyra_core::users::privset_from_actions(&actions);
        let flags = if grant {
            current | delta
        } else {
            current & !delta
        };
        rec.privilege = elyra_core::users::tier_from_privset(flags);
        sess.commit_write(
            vec![
                (ugkey, elyra_core::users::encode_privset(flags)),
                (user_key(&name), encode_user(&rec)),
            ],
            vec![],
        )
        .await?;
        applied += 1;
        i = j;
        if matches!(toks.get(i), Some(Tok::Sym(','))) {
            i += 1;
        } else {
            break;
        }
    }
    if applied == 0 {
        return Err(Error::Parse("no grantee specified".into()));
    }
    Ok(QueryResult::Affected(0))
}

/// GRANT role[, role] TO user[, user]  /  REVOKE role[, role] FROM user[, user].
async fn role_membership(toks: &[Tok], sess: &Session, grant: bool) -> Result<QueryResult> {
    let sep = if grant { "to" } else { "from" };
    // Roles named between the verb and TO/FROM.
    let mut i = 1;
    let mut roles: Vec<String> = Vec::new();
    while i < toks.len() && !toks[i].is_word(sep) {
        if let Tok::Word(s) | Tok::Str(s) = &toks[i] {
            roles.push(s.clone());
        }
        i += 1;
    }
    if i >= toks.len() {
        return Err(Error::Parse(format!(
            "{} <role> {} <user>",
            if grant { "GRANT" } else { "REVOKE" },
            sep.to_uppercase()
        )));
    }
    i += 1; // past TO/FROM
            // Verify the roles exist.
    for r in &roles {
        if sess.get(role_flag_key(r)).await?.is_none() {
            return Err(Error::Query(format!("role '{r}' does not exist")));
        }
    }
    let mut puts = Vec::new();
    let mut deletes = Vec::new();
    let mut applied = 0u64;
    while let Some((user, j)) = parse_userspec(toks, i) {
        if sess.get(user_key(&user)).await?.is_none() {
            return Err(Error::Query(format!("user '{user}' does not exist")));
        }
        for r in &roles {
            if grant {
                puts.push((role_member_key(&user, r), vec![1]));
            } else {
                deletes.push(role_member_key(&user, r));
            }
        }
        applied += 1;
        i = j;
        if matches!(toks.get(i), Some(Tok::Sym(','))) {
            i += 1;
        } else {
            break;
        }
    }
    if applied == 0 || roles.is_empty() {
        return Err(Error::Parse(
            "role membership: expected roles and users".into(),
        ));
    }
    sess.commit_write(puts, deletes).await?;
    Ok(QueryResult::Affected(0))
}

async fn show_grants(toks: &[Tok], sess: &Session) -> Result<QueryResult> {
    // SHOW GRANTS [FOR user]
    let name = if toks.get(2).map(|t| t.is_word("for")).unwrap_or(false) {
        parse_userspec(toks, 3).map(|(n, _)| n)
    } else {
        None
    };
    let schema = Schema::new(vec![elyra_core::ColumnDef {
        name: "Grants".into(),
        ty: elyra_core::ColumnType::Text,
        nullable: false,
        collation: elyra_core::Collation::Ci,
    }]);
    let mut rows = Vec::new();
    let emit = |rows: &mut Vec<Vec<Value>>, user: &str, flags: u32| {
        let privs = elyra_core::users::privset_to_names(flags);
        rows.push(vec![Value::Text(format!(
            "GRANT {privs} ON *.* TO '{user}'"
        ))]);
    };
    // A user's global flag set (migrating legacy accounts from their tier).
    async fn global_flags(sess: &Session, user: &str, tier: Privilege) -> Result<u32> {
        Ok(match sess.get(elyra_core::users::ugrant_key(user)).await? {
            Some(b) => elyra_core::users::decode_privset(&b),
            None => elyra_core::users::privset_from_tier(tier),
        })
    }
    // Append per-table (scoped) grants for a user.
    async fn emit_table_grants(
        sess: &Session,
        user: &str,
        rows: &mut Vec<Vec<Value>>,
    ) -> Result<()> {
        let prefix = table_grant_prefix(user);
        let batch = sess.scan_batch(prefix.clone(), None, 4096).await?;
        for (k, v) in &batch {
            let table = String::from_utf8_lossy(&k[prefix.len()..]).to_string();
            let p = decode_privilege(v).unwrap_or(Privilege::Read);
            let privs = match p {
                Privilege::Admin => "ALL PRIVILEGES",
                Privilege::Write => "SELECT, INSERT, UPDATE, DELETE",
                Privilege::Read => "SELECT",
            };
            rows.push(vec![Value::Text(format!(
                "GRANT {privs} ON `{table}` TO '{user}'"
            ))]);
        }
        Ok(())
    }

    match name {
        Some(n) => {
            let bytes = sess.get(user_key(&n)).await?;
            let rec = bytes
                .as_deref()
                .and_then(decode_user)
                .ok_or_else(|| Error::Query(format!("user '{n}' does not exist")))?;
            let flags = global_flags(sess, &n, rec.privilege).await?;
            emit(&mut rows, &n, flags);
            emit_table_grants(sess, &n, &mut rows).await?;
        }
        None => {
            // All users.
            let mut after: Option<Vec<u8>> = None;
            let mut names: Vec<(String, Privilege)> = Vec::new();
            loop {
                let batch = sess
                    .scan_batch(USER_PREFIX.to_vec(), after.clone(), 512)
                    .await?;
                if batch.is_empty() {
                    break;
                }
                for (k, v) in &batch {
                    let user = String::from_utf8_lossy(&k[USER_PREFIX.len()..]).to_string();
                    if let Some(rec) = decode_user(v) {
                        names.push((user, rec.privilege));
                    }
                }
                after = batch.last().map(|(k, _)| k.clone());
                if batch.len() < 512 {
                    break;
                }
            }
            for (user, p) in &names {
                let flags = global_flags(sess, user, *p).await?;
                emit(&mut rows, user, flags);
                emit_table_grants(sess, user, &mut rows).await?;
            }
        }
    }
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}
