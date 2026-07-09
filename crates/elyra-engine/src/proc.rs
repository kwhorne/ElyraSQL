//! A small procedural interpreter for stored-procedure bodies: `DECLARE`, `SET`,
//! `IF ... THEN ... [ELSEIF ...] [ELSE ...] END IF`, `WHILE ... DO ... END
//! WHILE`, and plain SQL statements. Parameters and local variables are held in
//! an environment and spliced into expressions and SQL as literals.
//!
//! This is a pragmatic subset, not full SQL/PSM: variables are referenced by
//! bare name (avoid names that clash with column names), and control-flow
//! keywords are matched case-insensitively.

use std::collections::HashMap;

use elyra_core::{Error, Result, Value};
use serde::{Deserialize, Serialize};

/// Parameter passing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParamMode {
    In,
    Out,
    Inout,
}

/// A stored procedure: its parameters (name + mode, in order) and body text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcDef {
    pub params: Vec<(String, ParamMode)>,
    pub body: String,
}

/// Control-flow signal returned by executing a procedural statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Flow {
    Normal,
    Leave(String),
    Iterate(String),
    /// An EXIT handler fired: unwind the whole procedure body.
    Exit,
}

/// A condition a handler responds to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cond {
    NotFound,
    SqlException,
    SqlState(String),
    Code(u64),
}

/// What a handler does after running its action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerKind {
    Continue,
    Exit,
}

/// A declared condition handler.
#[derive(Debug, Clone)]
pub struct Handler {
    pub kind: HandlerKind,
    pub conds: Vec<Cond>,
    pub action: Box<ProcStmt>,
}

impl Handler {
    /// Whether this handler responds to the given condition (`not_found` picks
    /// NOT FOUND / SQLSTATE 02000; otherwise any SQL error).
    pub fn matches(&self, not_found: bool) -> bool {
        self.conds.iter().any(|c| match c {
            Cond::NotFound => not_found,
            Cond::SqlState(s) => (s == "02000") == not_found,
            Cond::SqlException | Cond::Code(_) => !not_found,
        })
    }
}

/// An open cursor's materialized result set and read position.
pub struct Cursor {
    pub rows: Vec<Vec<Value>>,
    pub pos: usize,
}

/// Mutable procedure runtime context: declared cursors and handlers.
#[derive(Default)]
pub struct ProcCtx {
    pub cursor_defs: HashMap<String, String>,
    pub cursors: HashMap<String, Cursor>,
    pub handlers: Vec<Handler>,
}

/// A parsed procedural statement.
#[derive(Debug, Clone)]
pub enum ProcStmt {
    Declare {
        name: String,
        default: Option<String>,
    },
    Set {
        name: String,
        expr: String,
    },
    If {
        branches: Vec<(String, Vec<ProcStmt>)>,
        els: Option<Vec<ProcStmt>>,
    },
    While {
        label: Option<String>,
        cond: String,
        body: Vec<ProcStmt>,
    },
    Loop {
        label: Option<String>,
        body: Vec<ProcStmt>,
    },
    Repeat {
        label: Option<String>,
        body: Vec<ProcStmt>,
        until: String,
    },
    Leave(String),
    Iterate(String),
    /// `DECLARE cur CURSOR FOR <select>`
    DeclareCursor {
        name: String,
        query: String,
    },
    /// `OPEN cur`
    OpenCursor(String),
    /// `FETCH cur INTO a, b`
    Fetch {
        cursor: String,
        vars: Vec<String>,
    },
    /// `CLOSE cur`
    CloseCursor(String),
    /// `DECLARE {CONTINUE|EXIT} HANDLER FOR <cond>[, ...] <action>`
    DeclareHandler(Handler),
    Sql(String),
}

/// Split `body` into `;`-separated parts, respecting single-quoted strings.
fn split_parts(body: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            cur.push(c);
            while let Some(&n) = chars.peek() {
                cur.push(n);
                chars.next();
                if n == '\'' {
                    if chars.peek() == Some(&'\'') {
                        cur.push('\'');
                        chars.next();
                        continue;
                    }
                    break;
                }
            }
            continue;
        }
        if c == ';' {
            let t = cur.trim().to_string();
            if !t.is_empty() {
                parts.push(t);
            }
            cur.clear();
        } else {
            cur.push(c);
        }
    }
    let t = cur.trim().to_string();
    if !t.is_empty() {
        parts.push(t);
    }
    parts
}

fn kw_at(s: &str, kw: &str) -> bool {
    s.len() >= kw.len() && s[..kw.len()].eq_ignore_ascii_case(kw)
}

/// Classify an inline statement (after THEN/ELSE/DO/LOOP) into a ProcStmt.
fn inline_stmt(s: &str) -> ProcStmt {
    let low = s.to_ascii_lowercase();
    if low.starts_with("leave ") {
        ProcStmt::Leave(s[6..].trim().to_string())
    } else if low.starts_with("iterate ") {
        ProcStmt::Iterate(s[8..].trim().to_string())
    } else if low.starts_with("set ") {
        let rest = &s[4..];
        if let Some(eq) = rest.find('=') {
            ProcStmt::Set {
                name: rest[..eq].trim().to_string(),
                expr: rest[eq + 1..].trim().to_string(),
            }
        } else {
            ProcStmt::Sql(s.to_string())
        }
    } else {
        ProcStmt::Sql(s.to_string())
    }
}

/// Parse `DECLARE {CONTINUE|EXIT} HANDLER FOR <cond>[, ...] <action>`.
fn parse_handler(part: &str) -> Result<ProcStmt> {
    let rest = part["declare".len()..].trim();
    let low = rest.to_ascii_lowercase();
    let kind = if low.starts_with("exit") {
        HandlerKind::Exit
    } else {
        HandlerKind::Continue
    };
    let for_pos = low
        .find(" for ")
        .ok_or_else(|| Error::Parse("HANDLER requires FOR".into()))?;
    let after = rest[for_pos + " for ".len()..].trim();
    // Consume condition tokens; the first token that is not part of a condition
    // (nor a comma) begins the action.
    let bytes: Vec<&str> = after.split_whitespace().collect();
    let mut conds = Vec::new();
    let mut k = 0;
    while k < bytes.len() {
        let w = bytes[k].trim_end_matches(',').to_ascii_lowercase();
        match w.as_str() {
            "not"
                if k + 1 < bytes.len()
                    && bytes[k + 1]
                        .trim_end_matches(',')
                        .eq_ignore_ascii_case("found") =>
            {
                conds.push(Cond::NotFound);
                k += 2;
            }
            "sqlexception" => {
                conds.push(Cond::SqlException);
                k += 1;
            }
            "sqlwarning" => {
                conds.push(Cond::SqlException);
                k += 1;
            }
            "sqlstate" if k + 1 < bytes.len() => {
                let s = bytes[k + 1].trim_matches(['\'', '"', ',']).to_string();
                conds.push(Cond::SqlState(s));
                k += 2;
            }
            other if other.parse::<u64>().is_ok() => {
                conds.push(Cond::Code(other.parse().unwrap()));
                k += 1;
            }
            _ => break,
        }
    }
    if conds.is_empty() {
        return Err(Error::Parse("HANDLER requires a condition".into()));
    }
    let action_text = bytes[k..].join(" ");
    if action_text.trim().is_empty() {
        return Err(Error::Parse("HANDLER requires an action".into()));
    }
    Ok(ProcStmt::DeclareHandler(Handler {
        kind,
        conds,
        action: Box::new(inline_stmt(action_text.trim())),
    }))
}

/// Insert `;` boundaries around control-flow keywords so that block headers and
/// their first inner statement are separate parts (outside string literals).
fn normalize(body: &str) -> String {
    // Keyword lists (lowercase, whole-word).
    const AFTER: &[&str] = &["then", "do", "begin", "loop", "else"];
    const BEFORE: &[&str] = &["elseif", "until", "end", "else"];
    let cs: Vec<char> = body.chars().collect();
    let mut out = String::with_capacity(body.len() + 16);
    let ensure_semi = |out: &mut String| {
        let t = out.trim_end();
        if !t.is_empty() && !t.ends_with(';') {
            out.truncate(t.len());
            out.push(';');
        }
    };
    let mut i = 0;
    let mut prev = String::new();
    while i < cs.len() {
        let c = cs[i];
        if c == '\'' {
            out.push(c);
            i += 1;
            while i < cs.len() {
                out.push(cs[i]);
                if cs[i] == '\'' {
                    if i + 1 < cs.len() && cs[i + 1] == '\'' {
                        out.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c.is_ascii_alphabetic() {
            let start = i;
            while i < cs.len() && (cs[i].is_ascii_alphanumeric() || cs[i] == '_') {
                i += 1;
            }
            let word: String = cs[start..i].iter().collect();
            let low = word.to_ascii_lowercase();
            if BEFORE.contains(&low.as_str()) && prev != "end" {
                ensure_semi(&mut out);
                out.push(' ');
            }
            out.push_str(&word);
            // Don't break `END LOOP`/`END REPEAT` (a closing keyword after END).
            if AFTER.contains(&low.as_str()) && prev != "end" {
                out.push(';');
            }
            prev = low;
            continue;
        }
        if !c.is_whitespace() {
            prev.clear();
        }
        out.push(c);
        i += 1;
    }
    out
}

/// Parse a procedure body into procedural statements.
pub fn parse(body: &str) -> Result<Vec<ProcStmt>> {
    let parts = split_parts(&normalize(body));
    let mut i = 0;
    parse_block(&parts, &mut i, &[])
}

/// Parse statements until one of `terminators` (e.g. `END IF`) is reached; the
/// cursor is left on the terminator part.
fn parse_block(parts: &[String], i: &mut usize, terminators: &[&str]) -> Result<Vec<ProcStmt>> {
    let mut out = Vec::new();
    while *i < parts.len() {
        let part = parts[*i].clone();
        let low = part.to_ascii_lowercase();
        if terminators.iter().any(|t| low.starts_with(t)) {
            return Ok(out);
        }
        // Optional statement label: `name: WHILE|LOOP|REPEAT ...`.
        let (label, part) = match part.find(':') {
            Some(cp)
                if !part[..cp].trim().is_empty()
                    && part[..cp]
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_')
                    && {
                        let after = part[cp + 1..].trim_start().to_ascii_lowercase();
                        after.starts_with("while")
                            || after.starts_with("loop")
                            || after.starts_with("repeat")
                    } =>
            {
                (
                    Some(part[..cp].trim().to_string()),
                    part[cp + 1..].trim_start().to_string(),
                )
            }
            _ => (None, part),
        };
        if kw_at(&part, "loop") && (part.len() == 4 || part[4..].starts_with(char::is_whitespace)) {
            let inline = part[4..].trim().to_string();
            *i += 1;
            let mut body = Vec::new();
            if !inline.is_empty() {
                body.push(inline_stmt(&inline));
            }
            let mut rest = parse_block(parts, i, &["end loop"])?;
            body.append(&mut rest);
            if *i >= parts.len() {
                return Err(Error::Parse("LOOP without END LOOP".into()));
            }
            *i += 1;
            out.push(ProcStmt::Loop { label, body });
        } else if kw_at(&part, "repeat") {
            *i += 1;
            // REPEAT may have an inline first statement after the keyword.
            let mut body = Vec::new();
            let inline = part[6..].trim();
            if !inline.is_empty() {
                body.push(inline_stmt(inline));
            }
            let mut rest = parse_block(parts, i, &["until"])?;
            body.append(&mut rest);
            if *i >= parts.len() || !parts[*i].to_ascii_lowercase().starts_with("until") {
                return Err(Error::Parse("REPEAT without UNTIL".into()));
            }
            // `UNTIL <cond> END REPEAT` may be one part.
            let up = &parts[*i];
            let ulow = up.to_ascii_lowercase();
            let until = match ulow.find("end repeat") {
                Some(e) => up[5..e].trim().to_string(),
                None => up[5..].trim().to_string(),
            };
            if ulow.contains("end repeat") {
                *i += 1;
            } else {
                *i += 1;
                if *i < parts.len() && parts[*i].to_ascii_lowercase().starts_with("end repeat") {
                    *i += 1;
                }
            }
            out.push(ProcStmt::Repeat { label, body, until });
        } else if kw_at(&part, "while ") {
            out.push(parse_while(parts, i, label, &part)?);
        } else if kw_at(&part, "leave ") {
            out.push(ProcStmt::Leave(part[6..].trim().to_string()));
            *i += 1;
        } else if kw_at(&part, "iterate ") {
            out.push(ProcStmt::Iterate(part[8..].trim().to_string()));
            *i += 1;
        } else if kw_at(&part, "declare ") && low.contains(" cursor ") {
            // DECLARE <name> CURSOR FOR <select>
            let rest = part[8..].trim();
            let cpos = rest.to_ascii_lowercase().find(" cursor ").unwrap();
            let name = rest[..cpos].trim().to_string();
            let after = rest[cpos + " cursor ".len()..].trim();
            let query = match after.to_ascii_lowercase().strip_prefix("for ") {
                Some(_) => after[4..].trim().to_string(),
                None => after.to_string(),
            };
            out.push(ProcStmt::DeclareCursor { name, query });
            *i += 1;
        } else if kw_at(&part, "declare ") && low.contains(" handler ") {
            out.push(parse_handler(&part)?);
            *i += 1;
        } else if kw_at(&part, "open ") {
            out.push(ProcStmt::OpenCursor(part[5..].trim().to_string()));
            *i += 1;
        } else if kw_at(&part, "close ") {
            out.push(ProcStmt::CloseCursor(part[6..].trim().to_string()));
            *i += 1;
        } else if kw_at(&part, "fetch ") {
            // FETCH [NEXT FROM] cur INTO a, b
            let rest = part[6..].trim();
            let into = rest
                .to_ascii_lowercase()
                .find(" into ")
                .ok_or_else(|| Error::Parse("FETCH requires INTO".into()))?;
            let head = rest[..into].trim();
            let cursor = head
                .split_whitespace()
                .last()
                .unwrap_or(head)
                .trim_matches(['`', '"'])
                .to_string();
            let vars = rest[into + " into ".len()..]
                .split(',')
                .map(|v| v.trim().trim_start_matches('@').to_string())
                .filter(|v| !v.is_empty())
                .collect();
            out.push(ProcStmt::Fetch { cursor, vars });
            *i += 1;
        } else if kw_at(&part, "declare ") {
            let rest = part[8..].trim();
            // name type [DEFAULT expr]
            let name = rest
                .split_whitespace()
                .next()
                .ok_or_else(|| Error::Parse("DECLARE needs a name".into()))?
                .to_string();
            let default = low
                .find("default")
                .map(|p| part[p + "default".len()..].trim().to_string());
            out.push(ProcStmt::Declare { name, default });
            *i += 1;
        } else if kw_at(&part, "set ") {
            let rest = &part[4..];
            let eq = rest
                .find('=')
                .ok_or_else(|| Error::Parse("SET needs '='".into()))?;
            out.push(ProcStmt::Set {
                name: rest[..eq].trim().to_string(),
                expr: rest[eq + 1..].trim().to_string(),
            });
            *i += 1;
        } else if kw_at(&part, "if ") {
            out.push(parse_if(parts, i)?);
        } else {
            out.push(ProcStmt::Sql(part));
            *i += 1;
        }
    }
    Ok(out)
}

/// Split `IF <cond> THEN <maybe first stmt>` -> (cond, optional inline stmt).
fn split_header(part: &str, kw: &str, sep: &str) -> Result<(String, Option<String>)> {
    let after = part[kw.len()..].to_string();
    let low = after.to_ascii_lowercase();
    let sp = low
        .find(sep)
        .ok_or_else(|| Error::Parse(format!("expected {} in {}", sep.to_uppercase(), kw.trim())))?;
    let cond = after[..sp].trim().to_string();
    let inline = after[sp + sep.len()..].trim().to_string();
    Ok((cond, (!inline.is_empty()).then_some(inline)))
}

fn parse_if(parts: &[String], i: &mut usize) -> Result<ProcStmt> {
    let mut branches: Vec<(String, Vec<ProcStmt>)> = Vec::new();
    let mut els: Option<Vec<ProcStmt>> = None;
    // First IF ... THEN
    let (cond, inline) = split_header(&parts[*i], "if ", " then")?;
    *i += 1;
    let mut cur_cond = cond;
    let mut cur_body: Vec<ProcStmt> = Vec::new();
    if let Some(s) = inline {
        cur_body.push(inline_stmt(&s));
    }
    loop {
        let mut body = parse_block(parts, i, &["elseif ", "elseif", "else", "end if"])?;
        cur_body.append(&mut body);
        if *i >= parts.len() {
            return Err(Error::Parse("IF without END IF".into()));
        }
        let low = parts[*i].to_ascii_lowercase();
        if low.starts_with("elseif") {
            branches.push((std::mem::take(&mut cur_cond), std::mem::take(&mut cur_body)));
            let (c, inline) = split_header(&parts[*i], "elseif ", " then")?;
            cur_cond = c;
            if let Some(s) = inline {
                cur_body.push(inline_stmt(&s));
            }
            *i += 1;
        } else if low.starts_with("else") && !low.starts_with("elseif") {
            branches.push((std::mem::take(&mut cur_cond), std::mem::take(&mut cur_body)));
            // ELSE may have an inline first statement.
            let after = parts[*i][4..].trim().to_string();
            *i += 1;
            let mut eb = Vec::new();
            if !after.is_empty() {
                eb.push(inline_stmt(&after));
            }
            let mut rest = parse_block(parts, i, &["end if"])?;
            eb.append(&mut rest);
            els = Some(eb);
            break;
        } else {
            // end if
            branches.push((cur_cond, cur_body));
            break;
        }
    }
    // consume END IF
    *i += 1;
    Ok(ProcStmt::If { branches, els })
}

fn parse_while(
    parts: &[String],
    i: &mut usize,
    label: Option<String>,
    header: &str,
) -> Result<ProcStmt> {
    let (cond, inline) = split_header(header, "while ", " do")?;
    *i += 1;
    let mut body = Vec::new();
    if let Some(s) = inline {
        body.push(inline_stmt(&s));
    }
    let mut rest = parse_block(parts, i, &["end while"])?;
    body.append(&mut rest);
    if *i >= parts.len() {
        return Err(Error::Parse("WHILE without END WHILE".into()));
    }
    *i += 1; // consume END WHILE
    Ok(ProcStmt::While { label, cond, body })
}
