//! A small procedural interpreter for stored-procedure bodies: `DECLARE`, `SET`,
//! `IF ... THEN ... [ELSEIF ...] [ELSE ...] END IF`, `WHILE ... DO ... END
//! WHILE`, and plain SQL statements. Parameters and local variables are held in
//! an environment and spliced into expressions and SQL as literals.
//!
//! This is a pragmatic subset, not full SQL/PSM: variables are referenced by
//! bare name (avoid names that clash with column names), and control-flow
//! keywords are matched case-insensitively.

use elyra_core::{Error, Result};
use serde::{Deserialize, Serialize};

/// A stored procedure: its parameter names (in order) and body text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcDef {
    pub params: Vec<String>,
    pub body: String,
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
        cond: String,
        body: Vec<ProcStmt>,
    },
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

/// Parse a procedure body into procedural statements.
pub fn parse(body: &str) -> Result<Vec<ProcStmt>> {
    let parts = split_parts(body);
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
        if kw_at(&part, "declare ") {
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
        } else if kw_at(&part, "while ") {
            out.push(parse_while(parts, i)?);
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
        cur_body.push(ProcStmt::Sql(s));
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
                cur_body.push(ProcStmt::Sql(s));
            }
            *i += 1;
        } else if low.starts_with("else") && !low.starts_with("elseif") {
            branches.push((std::mem::take(&mut cur_cond), std::mem::take(&mut cur_body)));
            // ELSE may have an inline first statement.
            let after = parts[*i][4..].trim().to_string();
            *i += 1;
            let mut eb = Vec::new();
            if !after.is_empty() {
                eb.push(ProcStmt::Sql(after));
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

fn parse_while(parts: &[String], i: &mut usize) -> Result<ProcStmt> {
    let (cond, inline) = split_header(&parts[*i], "while ", " do")?;
    *i += 1;
    let mut body = Vec::new();
    if let Some(s) = inline {
        body.push(ProcStmt::Sql(s));
    }
    let mut rest = parse_block(parts, i, &["end while"])?;
    body.append(&mut rest);
    if *i >= parts.len() {
        return Err(Error::Parse("WHILE without END WHILE".into()));
    }
    *i += 1; // consume END WHILE
    Ok(ProcStmt::While { cond, body })
}
