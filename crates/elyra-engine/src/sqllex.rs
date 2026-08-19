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
//!   `/*! … */` version comment, but no rewriter here needs to reach inside one,
//!   so it is scanned as an ordinary comment: content is never rewritten.
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
