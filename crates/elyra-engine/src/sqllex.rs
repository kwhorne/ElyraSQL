//! Lexical scanning for the SQL string-rewriting pass.
//!
//! Several rewriters run over raw SQL *before* it reaches the parser, to cover
//! MySQL syntax `sqlparser` does not accept. Each of them needs the same thing:
//! walk the statement and act only on positions that are **code** — outside
//! string literals, quoted identifiers and comments — usually at parenthesis
//! depth zero.
//!
//! Every rewriter used to carry its own copy of that walk. Seven copies, none of
//! which understood backslash escapes, which is MySQL's default and what PDO and
//! `mysql_real_escape_string` emit. A literal like `'O\'Brien'` closed early, and
//! everything after it was treated as code:
//!
//! ```sql
//! SELECT 'a\'b!c'                    -- the `!` was rewritten to (NOT ...)
//! INSERT INTO t SET n = 'O\'B, Jr.'  -- the comma split the assignment list
//! ```
//!
//! The first returned a silently wrong string; the second was rejected as a
//! syntax error. Both are valid MySQL. This module is the single scanner they
//! all use now.
//!
//! **Quoting rules** follow MySQL with the default `sql_mode`:
//!
//! * `'…'` and `"…"` end on an unescaped matching quote. A backslash escapes the
//!   next byte, and a doubled quote (`''`) is a literal quote, not a close.
//!   `NO_BACKSLASH_ESCAPES` is not implemented by this server, so backslash is
//!   always an escape; if that mode is ever added, [`Quoting`] is where it goes.
//! * `` `…` `` ends on an unescaped backtick. Backticks take **no** backslash
//!   escape — `` `a\` `` is a complete identifier — but a doubled backtick is a
//!   literal one.
//! * `-- ` and `#` run to end of line; `--` needs whitespace or end of input
//!   after it, so `a--b` is arithmetic, not a comment.
//! * `/* … */` runs to the closing delimiter. MySQL *executes* the body of a
//!   `/*! … */` version comment; [`expand_version_comments`] turns the ones
//!   this server runs into code before any rewriter sees the statement, so the
//!   scanner here only meets those it does not, and skips them as comments.
//!
//! Nothing here allocates per byte, and nothing panics on arbitrary input — a
//! property test and the `preprocess` fuzz target both depend on that.

/// Where the scanner is inside the statement.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Quoting {
    Code,
    Single,
    Double,
    Backtick,
    LineComment,
    BlockComment,
}

/// One byte of the statement that is code: not inside a literal, a quoted
/// identifier or a comment.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CodeByte {
    /// Byte offset into the statement. Always a `char` boundary: every byte of
    /// a multi-byte UTF-8 sequence has its high bit set, so it can never equal
    /// one of the ASCII delimiters this scanner reacts to.
    pub index: usize,
    /// The byte itself.
    pub byte: u8,
    /// Parenthesis nesting at this byte, counting only parentheses that are
    /// themselves code. The depth of a `(` is the depth *inside* it, and the
    /// depth of its `)` is the same, so a top-level `(a, b)` reports depth 1 for
    /// both parentheses and for the comma between them.
    pub depth: i32,
}

/// Iterator over the code bytes of a statement. Built by [`code_bytes`].
pub struct CodeBytes<'a> {
    bytes: &'a [u8],
    index: usize,
    state: Quoting,
    depth: i32,
}

/// Walk the code bytes of `sql`, skipping literals, quoted identifiers and
/// comments.
pub fn code_bytes(sql: &str) -> CodeBytes<'_> {
    CodeBytes {
        bytes: sql.as_bytes(),
        index: 0,
        state: Quoting::Code,
        depth: 0,
    }
}

impl Iterator for CodeBytes<'_> {
    type Item = CodeByte;

    fn next(&mut self) -> Option<CodeByte> {
        while self.index < self.bytes.len() {
            let i = self.index;
            let b = self.bytes[i];
            let next = self.bytes.get(i + 1).copied();
            match self.state {
                Quoting::Code => {
                    self.index += 1;
                    match b {
                        b'\'' => self.state = Quoting::Single,
                        b'"' => self.state = Quoting::Double,
                        b'`' => self.state = Quoting::Backtick,
                        b'#' => self.state = Quoting::LineComment,
                        // `--` is a comment only when followed by whitespace or
                        // end of input; `5--3` is a subtraction.
                        b'-' if next == Some(b'-')
                            && self
                                .bytes
                                .get(i + 2)
                                .is_none_or(|c| c.is_ascii_whitespace()) =>
                        {
                            self.state = Quoting::LineComment;
                            self.index += 1;
                        }
                        b'/' if next == Some(b'*') => {
                            self.state = Quoting::BlockComment;
                            self.index += 1;
                        }
                        _ => {
                            let depth = self.depth;
                            if b == b'(' {
                                self.depth += 1;
                            } else if b == b')' {
                                self.depth -= 1;
                            }
                            // `(` reports the depth inside it and `)` the depth
                            // it closes, so a matched pair reads the same.
                            let depth = if b == b'(' { self.depth } else { depth };
                            return Some(CodeByte {
                                index: i,
                                byte: b,
                                depth,
                            });
                        }
                    }
                }
                Quoting::Single | Quoting::Double | Quoting::Backtick => {
                    let quote = match self.state {
                        Quoting::Single => b'\'',
                        Quoting::Double => b'"',
                        _ => b'`',
                    };
                    // Backticks take no backslash escape: `` `a\` `` is closed.
                    if b == b'\\' && self.state != Quoting::Backtick {
                        // Skip the escaped byte. A trailing backslash just ends
                        // the input; it must not step past the slice.
                        self.index = (i + 2).min(self.bytes.len());
                    } else if b == quote && next == Some(quote) {
                        // A doubled quote is one literal quote, not a close.
                        self.index = i + 2;
                    } else if b == quote {
                        self.state = Quoting::Code;
                        self.index = i + 1;
                    } else {
                        self.index = i + 1;
                    }
                }
                Quoting::LineComment => {
                    self.index = i + 1;
                    if b == b'\n' {
                        self.state = Quoting::Code;
                    }
                }
                Quoting::BlockComment => {
                    if b == b'*' && next == Some(b'/') {
                        self.state = Quoting::Code;
                        self.index = i + 2;
                    } else {
                        self.index = i + 1;
                    }
                }
            }
        }
        None
    }
}

/// The `MYSQL_VERSION_ID` this server answers to, from the version it reports
/// (`8.0.12-ElyraSQL-...` is 80012), for version comments.
pub fn mysql_version_id() -> u32 {
    let mut parts = elyra_core::SERVER_VERSION
        .split(|c: char| !c.is_ascii_digit())
        .map(|p| p.parse::<u32>().unwrap_or(0));
    let (major, minor, patch) = (
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
    );
    major * 10_000 + minor * 100 + patch
}

/// Run MySQL's version comments as MySQL does: the body of `/*! … */` is code,
/// and so is the body of `/*!NNNNN … */` when `NNNNN` (five or six digits,
/// `80012` = 8.0.12) is at most `server`; a later version's stays a comment.
/// Returns `None` when `sql` has no version comment to run.
///
/// `mysqldump` writes every view, trigger header and most session settings
/// this way. They used to be skipped as comments: a dump restored without an
/// error and without its views.
///
/// The delimiters of a comment that runs become spaces, so byte offsets inside
/// the body are unchanged. Quoting inside the body follows the rules above, so
/// a `*/` inside a string literal does not end it.
pub fn expand_version_comments(sql: &str, server: u32) -> Option<String> {
    let b = sql.as_bytes();
    if !sql.contains("/*!") {
        return None;
    }
    let mut out = String::with_capacity(sql.len());
    let mut state = Quoting::Code;
    // Inside the body of a version comment that runs.
    let mut in_version = false;
    let mut changed = false;
    let mut i = 0usize;
    let mut copied = 0usize;
    while i < b.len() {
        let c = b[i];
        let next = b.get(i + 1).copied();
        match state {
            Quoting::Code => match c {
                b'\'' => {
                    state = Quoting::Single;
                    i += 1;
                }
                b'"' => {
                    state = Quoting::Double;
                    i += 1;
                }
                b'`' => {
                    state = Quoting::Backtick;
                    i += 1;
                }
                b'#' => {
                    state = Quoting::LineComment;
                    i += 1;
                }
                b'-' if next == Some(b'-')
                    && b.get(i + 2).is_none_or(|c| c.is_ascii_whitespace()) =>
                {
                    state = Quoting::LineComment;
                    i += 2;
                }
                b'*' if in_version && next == Some(b'/') => {
                    out.push_str(&sql[copied..i]);
                    out.push_str("  ");
                    i += 2;
                    copied = i;
                    in_version = false;
                    changed = true;
                }
                b'/' if next == Some(b'*') && b.get(i + 2) == Some(&b'!') && !in_version => {
                    let digits = b[i + 3..].iter().take_while(|d| d.is_ascii_digit()).count();
                    // Five digits, or six; a longer run is a five-digit version
                    // followed by a number.
                    let len = match digits {
                        0 => 0,
                        6 => 6,
                        n => n.min(5),
                    };
                    let version: u32 = sql[i + 3..i + 3 + len].parse().unwrap_or(0);
                    if len == 0 || version <= server {
                        out.push_str(&sql[copied..i]);
                        out.push_str(&" ".repeat(3 + len));
                        i += 3 + len;
                        copied = i;
                        in_version = true;
                        changed = true;
                    } else {
                        state = Quoting::BlockComment;
                        i += 2;
                    }
                }
                b'/' if next == Some(b'*') => {
                    state = Quoting::BlockComment;
                    i += 2;
                }
                _ => i += 1,
            },
            Quoting::Single | Quoting::Double | Quoting::Backtick => {
                let quote = match state {
                    Quoting::Single => b'\'',
                    Quoting::Double => b'"',
                    _ => b'`',
                };
                if c == b'\\' && state != Quoting::Backtick {
                    i = (i + 2).min(b.len());
                } else if c == quote && next == Some(quote) {
                    i += 2;
                } else {
                    if c == quote {
                        state = Quoting::Code;
                    }
                    i += 1;
                }
            }
            Quoting::LineComment => {
                if c == b'\n' {
                    state = Quoting::Code;
                }
                i += 1;
            }
            Quoting::BlockComment => {
                if c == b'*' && next == Some(b'/') {
                    state = Quoting::Code;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        }
    }
    if !changed {
        return None;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

/// A query whose text is not UTF-8, made into one that is. Bytes that are not
/// UTF-8 can only be data, inside a string literal; each literal holding them
/// becomes the hex literal of its bytes, its escapes applied: `'\0\xff'` is
/// `X'00ff'`. mysqldump writes every BLOB value this way, as `_binary '...'`
/// with the raw bytes. `None` when such bytes stand anywhere else.
pub fn binary_literals_as_hex(sql: &[u8]) -> Option<String> {
    let mut out: Vec<u8> = Vec::with_capacity(sql.len() * 2);
    let mut state = Quoting::Code;
    let mut literal_start = 0usize;
    let mut copied = 0usize;
    let mut i = 0usize;
    while i < sql.len() {
        let c = sql[i];
        let next = sql.get(i + 1).copied();
        match state {
            Quoting::Code => match c {
                b'\'' | b'"' => {
                    state = if c == b'\'' {
                        Quoting::Single
                    } else {
                        Quoting::Double
                    };
                    literal_start = i;
                    i += 1;
                }
                b'`' => {
                    state = Quoting::Backtick;
                    i += 1;
                }
                b'#' => {
                    state = Quoting::LineComment;
                    i += 1;
                }
                b'-' if next == Some(b'-')
                    && sql.get(i + 2).is_none_or(|c| c.is_ascii_whitespace()) =>
                {
                    state = Quoting::LineComment;
                    i += 2;
                }
                b'/' if next == Some(b'*') => {
                    state = Quoting::BlockComment;
                    i += 2;
                }
                _ => i += 1,
            },
            Quoting::Single | Quoting::Double | Quoting::Backtick => {
                let quote = match state {
                    Quoting::Single => b'\'',
                    Quoting::Double => b'"',
                    _ => b'`',
                };
                if c == b'\\' && state != Quoting::Backtick {
                    i = (i + 2).min(sql.len());
                } else if c == quote && next == Some(quote) {
                    i += 2;
                } else if c == quote {
                    let body = &sql[literal_start + 1..i];
                    if state != Quoting::Backtick && std::str::from_utf8(body).is_err() {
                        out.extend_from_slice(&sql[copied..literal_start]);
                        out.extend_from_slice(b"X'");
                        for byte in unescape_literal(body, quote) {
                            out.extend_from_slice(format!("{byte:02x}").as_bytes());
                        }
                        out.push(b'\'');
                        copied = i + 1;
                    }
                    state = Quoting::Code;
                    i += 1;
                } else {
                    i += 1;
                }
            }
            Quoting::LineComment => {
                if c == b'\n' {
                    state = Quoting::Code;
                }
                i += 1;
            }
            Quoting::BlockComment => {
                if c == b'*' && next == Some(b'/') {
                    state = Quoting::Code;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        }
    }
    out.extend_from_slice(&sql[copied..]);
    String::from_utf8(out).ok()
}

/// The bytes a MySQL string literal's body stands for: `\0`, `\b`, `\n`,
/// `\r`, `\t` and `\Z` are control bytes, `\%` and `\_` keep their backslash
/// (they are `LIKE` escapes), any other escaped byte is itself, and a doubled
/// quote is one.
fn unescape_literal(body: &[u8], quote: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len());
    let mut i = 0;
    while i < body.len() {
        let c = body[i];
        if c == b'\\' && i + 1 < body.len() {
            match body[i + 1] {
                b'0' => out.push(0),
                b'b' => out.push(8),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'Z' => out.push(26),
                e @ (b'%' | b'_') => out.extend_from_slice(&[b'\\', e]),
                e => out.push(e),
            }
            i += 2;
        } else if c == quote && body.get(i + 1) == Some(&quote) {
            out.push(quote);
            i += 2;
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
}

/// Split `sql` on every top-level code occurrence of `sep`, which must be ASCII.
///
/// Joining the parts back with `sep` reproduces the input exactly, so a caller
/// can rewrite one part and reassemble without losing bytes.
pub fn split_top_level(sql: &str, sep: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0usize;
    if sep.is_ascii() {
        let sep = sep as u8;
        for c in code_bytes(sql) {
            if c.depth == 0 && c.byte == sep {
                out.push(sql[start..c.index].to_string());
                start = c.index + 1;
            }
        }
    }
    out.push(sql[start..].to_string());
    out
}

/// Byte offset of the first top-level code byte satisfying `want`, which is
/// given the whole statement and the candidate's offset so it can look ahead.
pub fn find_top_level(sql: &str, want: impl Fn(&str, CodeByte) -> bool) -> Option<usize> {
    code_bytes(sql)
        .find(|&c| c.depth == 0 && want(sql, c))
        .map(|c| c.index)
}

/// Byte offset of the **last** top-level code byte satisfying `want`.
///
/// Scans forward and keeps the final match: quoting state depends on everything
/// before a byte, so the scan cannot run backwards.
pub fn rfind_top_level(sql: &str, want: impl Fn(&str, CodeByte) -> bool) -> Option<usize> {
    code_bytes(sql)
        .filter(|&c| c.depth == 0 && want(sql, c))
        .map(|c| c.index)
        .last()
}

/// True when the code keyword `kw` starts at byte `at`, as a whole word.
///
/// Matching is case-insensitive, and the neighbouring bytes must not be
/// identifier characters, so `SET` does not match inside `OFFSET` or `SETTING`.
pub fn keyword_at(sql: &str, at: usize, kw: &str) -> bool {
    let b = sql.as_bytes();
    let Some(window) = b.get(at..at + kw.len()) else {
        return false;
    };
    if !window.eq_ignore_ascii_case(kw.as_bytes()) {
        return false;
    }
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$';
    let before_ok = at == 0 || !ident(b[at - 1]);
    let after_ok = b.get(at + kw.len()).is_none_or(|&c| !ident(c));
    before_ok && after_ok
}

/// End (exclusive) of the parenthesised group opening at byte `start`, or
/// `None` if it is never closed. `start` must be the `(` itself.
pub fn matching_paren_end(sql: &str, start: usize) -> Option<usize> {
    if sql.as_bytes().get(start) != Some(&b'(') {
        return None;
    }
    let mut depth = 0i32;
    for c in code_bytes(sql) {
        if c.index < start {
            continue;
        }
        match c.byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(c.index + 1);
                }
            }
            _ => {}
        }
    }
    None
}

/// Keep the backslash of `\_` and `\%` inside string literals, as MySQL does.
///
/// MySQL's string escapes drop the backslash everywhere except before `_` and
/// `%`: `'a\_b'` is the three characters `a\_b`, so that a `LIKE` pattern can
/// match a literal underscore or percent sign. The SQL parser follows the
/// general rule and turned it into `a_b`, a wildcard -- `'axb' LIKE 'a\_b'`
/// was true, and a search escaping its input that way (Laravel's
/// `addcslashes($term, '%_')`) matched too much. Doubling the backslash before
/// parsing gives the parser `\\_`, which it reads back as `\_`.
///
/// A backslash pair (`\\`) is left as it is, so `'x\\_y'` still means `x\_y`.
/// `None` when nothing needs changing.
pub fn keep_like_escapes(sql: &str) -> Option<String> {
    if !sql.contains("\\_") && !sql.contains("\\%") {
        return None;
    }
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 8);
    let (mut i, mut copied) = (0usize, 0usize);
    let mut state = Quoting::Code;
    while i < b.len() {
        let next = b.get(i + 1).copied();
        match state {
            Quoting::Code => match b[i] {
                b'\'' => state = Quoting::Single,
                b'"' => state = Quoting::Double,
                b'`' => state = Quoting::Backtick,
                b'#' => state = Quoting::LineComment,
                b'-' if next == Some(b'-')
                    && b.get(i + 2).is_none_or(|c| c.is_ascii_whitespace()) =>
                {
                    state = Quoting::LineComment;
                    i += 1;
                }
                b'/' if next == Some(b'*') => {
                    state = Quoting::BlockComment;
                    i += 1;
                }
                _ => {}
            },
            Quoting::Single | Quoting::Double => {
                let quote = if state == Quoting::Single {
                    b'\''
                } else {
                    b'"'
                };
                if b[i] == b'\\' {
                    if matches!(next, Some(b'_' | b'%')) {
                        out.push_str(&sql[copied..i]);
                        out.push_str("\\\\");
                        copied = i + 1;
                    }
                    i += 2;
                    continue;
                }
                if b[i] == quote {
                    if next == Some(quote) {
                        i += 2;
                        continue;
                    }
                    state = Quoting::Code;
                }
            }
            Quoting::Backtick => {
                if b[i] == b'`' {
                    if next == Some(b'`') {
                        i += 2;
                        continue;
                    }
                    state = Quoting::Code;
                }
            }
            Quoting::LineComment => {
                if b[i] == b'\n' {
                    state = Quoting::Code;
                }
            }
            Quoting::BlockComment => {
                if b[i] == b'*' && next == Some(b'/') {
                    state = Quoting::Code;
                    i += 1;
                }
            }
        }
        i += 1;
    }
    if copied == 0 {
        return None;
    }
    out.push_str(&sql[copied..]);
    Some(out)
}

// --- Statement tokens -------------------------------------------------------
//
// A second, coarser level than the byte scanner above: a whole statement as a
// stream of words, quoted strings and symbols. Statements that the SQL frontend
// never sees are parsed from this -- user management (`GRANT`, `CREATE USER`)
// and embedding indexes -- so it lives here rather than in either of them, for
// the same reason the byte scanner does: one copy, not one per caller.
//
// Quoting is simpler than [`code_bytes`] deliberately. These statements are
// short, hand-written DDL, and the only escape MySQL requires in them is a
// doubled quote. A backslash is an ordinary character.

/// A single lexical token: a bare word/number, a quoted string, or a symbol.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Tok {
    Word(String),
    Str(String),
    Sym(char),
}

impl Tok {
    /// The textual value of a word or quoted string (symbols yield "").
    pub(crate) fn text(&self) -> &str {
        match self {
            Tok::Word(s) | Tok::Str(s) => s,
            Tok::Sym(_) => "",
        }
    }
    pub(crate) fn is_word(&self, kw: &str) -> bool {
        matches!(self, Tok::Word(s) if s.eq_ignore_ascii_case(kw))
    }
}

pub(crate) fn tokenize(sql: &str) -> Vec<Tok> {
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

#[cfg(test)]
mod binary_literal_tests {
    use super::binary_literals_as_hex;

    #[test]
    fn a_literal_that_is_not_utf8_becomes_its_bytes_in_hex() {
        let sql = b"INSERT INTO t VALUES (_binary '\\0\xff\\'x''',\"\xfe\\n\\_\")";
        assert_eq!(
            binary_literals_as_hex(sql).as_deref(),
            Some("INSERT INTO t VALUES (_binary X'00ff277827',X'fe0a5c5f')")
        );
    }

    #[test]
    fn utf8_literals_identifiers_and_comments_are_left_as_they_are() {
        let sql = "SELECT 'æøå', `a\\b`, \"x\" -- '\u{fffd}'\n/* 'y' */";
        assert_eq!(binary_literals_as_hex(sql.as_bytes()).as_deref(), Some(sql));
    }

    #[test]
    fn bytes_that_are_not_utf8_outside_a_literal_are_refused() {
        for sql in [
            &b"SELECT 1 \xff"[..],
            b"SELECT `\xff`",
            b"SELECT 1 -- \xff\n",
            b"SELECT '\xff",
        ] {
            assert_eq!(binary_literals_as_hex(sql), None, "{sql:?}");
        }
    }
}

#[cfg(test)]
mod like_escape_tests {
    use super::keep_like_escapes;

    #[test]
    fn backslash_underscore_and_percent_keep_their_backslash_in_literals() {
        assert_eq!(
            keep_like_escapes(r#"SELECT 'a\_b', 'x\%y', "z\_w""#).as_deref(),
            Some(r#"SELECT 'a\\_b', 'x\\%y', "z\\_w""#)
        );
        // A backslash pair stays as it is: 'x\\_y' already means x\_y.
        assert_eq!(keep_like_escapes(r"SELECT 'x\\_y'"), None);
        // Other escapes, doubled quotes and a quote escaped inside: untouched.
        assert_eq!(
            keep_like_escapes(r"SELECT 'it''s \_', 'it\'s \_', 'a\nb'").as_deref(),
            Some(r"SELECT 'it''s \\_', 'it\'s \\_', 'a\nb'")
        );
    }

    #[test]
    fn code_comments_and_identifiers_are_left_alone() {
        for sql in [
            r"SELECT `a\_b` FROM t",
            r"SELECT 1 -- '\_'",
            r"SELECT 1 # '\_'",
            r"SELECT 1 /* '\_' */",
            r"SELECT a\_b",
            "SELECT 'plain'",
        ] {
            assert_eq!(keep_like_escapes(sql), None, "{sql}");
        }
        // After a comment, literals are literals again.
        assert_eq!(
            keep_like_escapes("SELECT 1 /* x */, '\\_'").as_deref(),
            Some("SELECT 1 /* x */, '\\\\_'")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The code bytes of `sql`, as a string. Handy for asserting what the
    /// scanner considers rewritable.
    fn code(sql: &str) -> String {
        code_bytes(sql).map(|c| c.byte as char).collect()
    }

    #[test]
    fn literals_and_identifiers_are_not_code() {
        assert_eq!(code("a'b'c"), "ac");
        assert_eq!(code("a\"b\"c"), "ac");
        assert_eq!(code("a`b`c"), "ac");
    }

    #[test]
    fn backslash_escapes_do_not_end_a_literal() {
        // The bug this module exists for: after `\'` the scanner used to think
        // it was back in code, so `!`, `,` and keywords inside the literal were
        // rewritten.
        assert_eq!(code(r"x'a\'b!c'y"), "xy");
        assert_eq!(code(r"x'O\'Brien, Jr.'y"), "xy");
        assert_eq!(code(r#"x"a\"b"y"#), "xy");
        // A trailing backslash must not read past the end.
        assert_eq!(code(r"x'a\"), "x");
    }

    #[test]
    fn doubled_quotes_are_literal_not_a_close() {
        assert_eq!(code("x'it''s, ok'y"), "xy");
        assert_eq!(code(r#"x"a""b"y"#), "xy");
        assert_eq!(code("x`a``b`y"), "xy");
    }

    #[test]
    fn backticks_take_no_backslash_escape() {
        // MySQL closes this identifier at the second backtick; only the `y` is
        // code. Treating `\` as an escape here would swallow the close.
        assert_eq!(code(r"x`a\`y"), "xy");
    }

    #[test]
    fn comments_are_not_code() {
        assert_eq!(code("a -- b\nc"), "a c");
        assert_eq!(code("a # b\nc"), "a c");
        assert_eq!(code("a /* b */ c"), "a  c");
        assert_eq!(code("a -- b"), "a ");
        // `--` without following whitespace is arithmetic, not a comment.
        assert_eq!(code("5--3"), "5--3");
        // A quote inside a comment must not open a literal.
        assert_eq!(code("a -- it's\nb"), "a b");
        assert_eq!(code("a /* it's */ b"), "a  b");
    }

    #[test]
    fn depth_counts_only_code_parentheses() {
        let depths: Vec<i32> = code_bytes("f(a,')(',b)")
            .filter(|c| c.byte == b',')
            .map(|c| c.depth)
            .collect();
        // Both commas are inside the call; the parentheses in the literal are
        // not counted.
        assert_eq!(depths, vec![1, 1]);
    }

    #[test]
    fn version_comments_run_up_to_the_server_version() {
        let run = |sql: &str| {
            super::expand_version_comments(sql, 80012)
                .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        };
        // What mysqldump writes.
        assert_eq!(
            run("/*!50001 CREATE ALGORITHM=UNDEFINED */ /*!50013 DEFINER=`root`@`%` SQL SECURITY DEFINER */ /*!50001 VIEW `v` AS select 1 AS `x` */").as_deref(),
            Some("CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS select 1 AS `x`")
        );
        assert_eq!(
            run("/*!40101 SET NAMES utf8mb4 */").as_deref(),
            Some("SET NAMES utf8mb4")
        );
        assert_eq!(run("SELECT 1 /*! + 1 */").as_deref(), Some("SELECT 1 + 1"));
        // Six digits (8.0.34 and later spell the version so).
        assert_eq!(
            run("SELECT 1 /*!080000 + 1 */").as_deref(),
            Some("SELECT 1 + 1")
        );
        // A later version's body stays a comment.
        assert_eq!(
            run("/*!80016 SET @@session.default_table_encryption=0*/"),
            None
        );
        assert_eq!(
            run("/*!40101 SET @a = 1 */ /*!90000 SET @b = 2 */").as_deref(),
            Some("SET @a = 1 /*!90000 SET @b = 2 */")
        );
        // Quoting inside the body: a `*/` in a literal does not end it.
        assert_eq!(
            run("/*!50001 SELECT '*/' AS x */").as_deref(),
            Some("SELECT '*/' AS x")
        );
        // Outside code nothing changes.
        assert_eq!(run("SELECT '/*!50001 x */'"), None);
        assert_eq!(run("SELECT 1 /* plain */"), None);
        assert_eq!(run("SELECT 1 -- /*!50001 x */"), None);
    }

    #[test]
    fn mysql_version_id_is_the_reported_version() {
        assert_eq!(super::mysql_version_id(), 80012);
    }

    #[test]
    fn split_top_level_ignores_literals_and_nesting() {
        assert_eq!(split_top_level("a,b", ','), vec!["a", "b"]);
        assert_eq!(split_top_level("a,(b,c)", ','), vec!["a", "(b,c)"]);
        assert_eq!(split_top_level("'a,b',c", ','), vec!["'a,b'", "c"]);
        // The reported bug: an escaped quote must not expose the comma.
        assert_eq!(
            split_top_level(r"n = 'O\'Brien, Jr.', age = 5", ','),
            vec![r"n = 'O\'Brien, Jr.'", " age = 5"]
        );
    }

    #[test]
    fn split_top_level_roundtrips() {
        for s in [
            "",
            ",",
            "a,b,c",
            r"a = 'x\', y', b",
            "a /* , */ b",
            "unterminated 'literal",
        ] {
            assert_eq!(split_top_level(s, ',').join(","), s, "{s}");
        }
    }

    #[test]
    fn keyword_at_matches_whole_words_only() {
        assert!(keyword_at("INSERT INTO t SET x=1", 14, "SET"));
        assert!(keyword_at("insert into t set x=1", 14, "SET"));
        // Not inside a longer identifier.
        assert!(!keyword_at("SELECT x OFFSET 1", 9, "SET"));
        assert!(!keyword_at("SELECT setting", 7, "SET"));
        assert!(!keyword_at("x", 0, "SET"));
    }

    #[test]
    fn find_and_rfind_skip_literals() {
        let sql = r"a ! 'b ! c' ! d";
        let bang = |_: &str, c: CodeByte| c.byte == b'!';
        assert_eq!(find_top_level(sql, bang), Some(2));
        assert_eq!(rfind_top_level(sql, bang), Some(12));
        // Nothing outside a literal.
        assert_eq!(find_top_level("'!'", bang), None);
    }

    #[test]
    fn matching_paren_end_spans_literals_and_nesting() {
        assert_eq!(matching_paren_end("(a)", 0), Some(3));
        assert_eq!(matching_paren_end("(a(b))c", 0), Some(6));
        assert_eq!(matching_paren_end("(')')x", 0), Some(5));
        assert_eq!(matching_paren_end("(a", 0), None);
        // `start` must be the opening parenthesis.
        assert_eq!(matching_paren_end("a(b)", 0), None);
    }

    #[test]
    fn multibyte_input_yields_char_boundaries() {
        let sql = "SELECT 'æøå' , 'naïve' , x";
        for c in code_bytes(sql) {
            assert!(
                sql.is_char_boundary(c.index),
                "index {} splits a char",
                c.index
            );
        }
    }
}
