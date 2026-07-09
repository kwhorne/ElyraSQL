//! User management DDL: `CREATE USER`, `DROP USER`, `ALTER USER` /
//! `SET PASSWORD`, `GRANT`, `REVOKE`, and `SHOW GRANTS`.
//!
//! MySQL's user grammar isn't parsed by the SQL frontend, and privileges in
//! ElyraSQL are coarse (global Read / Write / Admin), so we parse these
//! statements here with a small tokenizer and store accounts in the database
//! under `sys::user::` — the same records the authenticator reads.

use elyra_core::users::{
    decode_privilege, decode_user, encode_privilege, encode_user, password_digest,
    privilege_from_actions, table_grant_key, table_grant_prefix, user_key, UserRecord, USER_PREFIX,
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

/// True if `sql` (already trimmed) begins a user-management statement we handle.
pub fn is_user_stmt(head: &str) -> bool {
    let h = head.trim_start();
    let starts = |kw: &str| h.len() >= kw.len() && h[..kw.len()].eq_ignore_ascii_case(kw);
    starts("create user")
        || starts("drop user")
        || starts("alter user")
        || starts("set password")
        || starts("grant ")
        || starts("revoke ")
        || starts("show grants")
        || h.eq_ignore_ascii_case("show grants")
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
    rec.digest = password_digest(pw.as_bytes());
    sess.commit_write(vec![(user_key(&name), encode_user(&rec))], vec![])
        .await?;
    Ok(QueryResult::Affected(0))
}

async fn grant_revoke(toks: &[Tok], sess: &Session, grant: bool) -> Result<QueryResult> {
    // Collect action words up to ON.
    let mut i = 1;
    let mut actions: Vec<String> = Vec::new();
    while i < toks.len() && !toks[i].is_word("on") {
        if let Tok::Word(s) = &toks[i] {
            actions.push(s.clone());
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
        rec.privilege = if grant {
            level.max(rec.privilege)
        } else {
            // REVOKE lowers the account to Read (coarse model).
            Privilege::Read
        };
        sess.commit_write(vec![(user_key(&name), encode_user(&rec))], vec![])
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
    let emit = |rows: &mut Vec<Vec<Value>>, user: &str, p: Privilege| {
        let privs = match p {
            Privilege::Admin => "ALL PRIVILEGES",
            Privilege::Write => "SELECT, INSERT, UPDATE, DELETE",
            Privilege::Read => "SELECT",
        };
        rows.push(vec![Value::Text(format!(
            "GRANT {privs} ON *.* TO '{user}'"
        ))]);
    };
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
            emit(&mut rows, &n, rec.privilege);
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
                emit(&mut rows, user, *p);
                emit_table_grants(sess, user, &mut rows).await?;
            }
        }
    }
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}
