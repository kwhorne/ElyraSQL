//! Minimal JSON model with path extraction, for the `JSON` type's `->`,
//! `->>`, and `JSON_EXTRACT` operators. Dependency-free.

/// A parsed JSON value. Numbers keep their original text to avoid precision
/// loss on round-trip.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

/// Maximum JSON nesting depth accepted by the parser. Bounds recursion (and the
/// resulting value's recursive `Drop`/serialisation) so a deeply-nested document
/// like `[[[[...]]]]` can't overflow the thread stack and abort the process.
/// Comfortably above MySQL's documented ~100-level limit; matches the on-write
/// validator in `value.rs`.
const MAX_JSON_DEPTH: usize = 200;

/// Parse a JSON document, its numbers kept as written. Returns `None` if it is
/// not valid JSON (including when it nests deeper than [`MAX_JSON_DEPTH`]).
///
/// For a JSON value: a JSON column's text or a JSON function's result, already
/// in MySQL's form, whose numbers may carry a DECIMAL's scale (`[1.50]` from
/// `JSON_ARRAY(1.50)`). JSON *text* -- a string literal, a parameter -- is read
/// with [`parse_text`].
pub fn parse(s: &str) -> Option<Json> {
    let b = s.as_bytes();
    let mut p = 0;
    skip_ws(b, &mut p);
    let v = parse_value(b, &mut p, 0)?;
    skip_ws(b, &mut p);
    if p == b.len() {
        Some(v)
    } else {
        None
    }
}

/// Parse JSON text as MySQL reads it: as [`parse`], a number then read as an
/// integer or a double and written the one way MySQL writes that (see
/// [`canonical_number`]), so `'[1.50, 1e2]'` is `[1.5, 100.0]`.
pub fn parse_text(s: &str) -> Option<Json> {
    let mut json = parse(s)?;
    json.canonicalize_numbers();
    Some(json)
}

/// `text` as MySQL stores and prints a JSON document, or `None` if it is not
/// valid JSON: no whitespace but `", "` and `": "`, an object's keys in MySQL's
/// order (shorter first, then by bytes) with the last of a duplicated key
/// kept, numbers and strings written the one way MySQL writes them (see
/// [`canonical_number`], `write_quoted`).
pub fn canonical(text: &str) -> Option<String> {
    parse_text(text).map(|json| json.to_json_string())
}

impl Json {
    fn canonicalize_numbers(&mut self) {
        match self {
            Json::Num(n) => *n = canonical_number(n),
            Json::Arr(items) => items.iter_mut().for_each(Json::canonicalize_numbers),
            Json::Obj(pairs) => pairs.iter_mut().for_each(|(_, v)| v.canonicalize_numbers()),
            _ => {}
        }
    }

    /// The document as MySQL writes it (see [`canonical`]).
    pub fn to_json_string(&self) -> String {
        let mut out = String::new();
        self.write(&mut out);
        out
    }

    /// Unquoted form: a scalar string is returned raw; composites are JSON.
    pub fn to_unquoted(&self) -> String {
        match self {
            Json::Str(s) => s.clone(),
            Json::Null => "null".into(),
            Json::Bool(b) => b.to_string(),
            Json::Num(n) => n.clone(),
            _ => self.to_json_string(),
        }
    }

    fn write(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Json::Num(n) => out.push_str(n),
            Json::Str(s) => write_quoted(s, out),
            Json::Arr(items) => {
                out.push('[');
                for (i, it) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    it.write(out);
                }
                out.push(']');
            }
            Json::Obj(pairs) => {
                out.push('{');
                for (i, (k, v)) in mysql_key_order(pairs).into_iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    write_quoted(k, out);
                    out.push_str(": ");
                    v.write(out);
                }
                out.push('}');
            }
        }
    }

    /// Navigate a MySQL-style path (`$`, `$.key`, `$[0]`, chained).
    pub fn extract(&self, path: &str) -> Option<Json> {
        let steps = parse_path(path)?;
        let mut cur = self;
        for step in steps {
            cur = match (cur, step) {
                (Json::Obj(pairs), Step::Key(k)) => &pairs.iter().find(|(kk, _)| *kk == k)?.1,
                (Json::Arr(items), Step::Index(i)) => items.get(i)?,
                _ => return None,
            };
        }
        Some(cur.clone())
    }

    /// MySQL `JSON_TYPE` name.
    pub fn type_name(&self) -> &'static str {
        match self {
            Json::Null => "NULL",
            Json::Bool(_) => "BOOLEAN",
            Json::Num(n) => {
                if n.contains('.') || n.contains('e') || n.contains('E') {
                    "DOUBLE"
                } else {
                    "INTEGER"
                }
            }
            Json::Str(_) => "STRING",
            Json::Arr(_) => "ARRAY",
            Json::Obj(_) => "OBJECT",
        }
    }

    /// `JSON_LENGTH`: element count for arrays/objects, else 1.
    pub fn length(&self) -> usize {
        match self {
            Json::Arr(a) => a.len(),
            Json::Obj(o) => o.len(),
            _ => 1,
        }
    }

    /// `JSON_KEYS`: object keys, or `None` for non-objects.
    pub fn keys(&self) -> Option<Vec<String>> {
        match self {
            Json::Obj(o) => Some(
                mysql_key_order(o)
                    .into_iter()
                    .map(|(k, _)| k.clone())
                    .collect(),
            ),
            _ => None,
        }
    }

    /// `JSON_CONTAINS`: does `self` contain `candidate` (MySQL semantics)?
    pub fn contains(&self, candidate: &Json) -> bool {
        match (self, candidate) {
            (Json::Obj(t), Json::Obj(c)) => c.iter().all(|(ck, cv)| {
                t.iter()
                    .find(|(tk, _)| tk == ck)
                    .is_some_and(|(_, tv)| tv.contains(cv))
            }),
            (Json::Arr(t), Json::Arr(c)) => c.iter().all(|ce| t.iter().any(|te| te.contains(ce))),
            (Json::Arr(t), c) => t.iter().any(|te| te.contains(c)),
            (t, c) => t.same_scalar(c),
        }
    }

    /// Equal as MySQL compares two JSON scalars: numbers by value, so `1.0`
    /// is `1` and `1.50` is `1.5`.
    fn same_scalar(&self, other: &Json) -> bool {
        match (self, other) {
            (Json::Num(a), Json::Num(b)) => same_number(a, b),
            _ => self == other,
        }
    }

    /// Build a JSON value from a SQL value (for `JSON_ARRAY`/`JSON_OBJECT`/...).
    pub fn from_value(v: &crate::value::Value) -> Json {
        use crate::value::Value;
        match v {
            Value::Null => Json::Null,
            Value::Bool(b) => Json::Bool(*b),
            Value::Int(i) => Json::Num(i.to_string()),
            Value::UInt(u) => Json::Num(u.to_string()),
            Value::Float(f) if f.is_finite() => Json::Num(format_double(*f)),
            // A DECIMAL is a number in JSON, with its scale, as in MySQL
            // (`JSON_ARRAY(1.50)` is `[1.50]`); it became the string "1.50".
            Value::Decimal(..) => match v.to_wire_string() {
                Some(text) => Json::Num(text),
                None => Json::Null,
            },
            Value::Text(s) => Json::Str(s.clone()),
            Value::Json(s) => parse(s).unwrap_or_else(|| Json::Str(s.clone())),
            other => Json::Str(other.to_wire_string().unwrap_or_default()),
        }
    }

    /// `JSON_SET`/`JSON_INSERT`/`JSON_REPLACE` at `path`. Returns whether the
    /// document changed.
    pub fn set_path(&mut self, path: &str, val: Json, mode: SetMode) -> bool {
        let Some(steps) = parse_path(path) else {
            return false;
        };
        if steps.is_empty() {
            if mode != SetMode::Insert {
                *self = val;
                return true;
            }
            return false;
        }
        set_recurse(self, &steps, val, mode)
    }

    /// `JSON_REMOVE` at `path`. Returns whether the document changed.
    pub fn remove_path(&mut self, path: &str) -> bool {
        let Some(steps) = parse_path(path) else {
            return false;
        };
        if steps.is_empty() {
            return false;
        }
        remove_recurse(self, &steps)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetMode {
    Set,
    Insert,
    Replace,
}

fn set_recurse(cur: &mut Json, steps: &[Step], val: Json, mode: SetMode) -> bool {
    if steps.len() == 1 {
        return match (cur, &steps[0]) {
            (Json::Obj(pairs), Step::Key(k)) => {
                if let Some(slot) = pairs.iter_mut().find(|p| &p.0 == k) {
                    if mode != SetMode::Insert {
                        slot.1 = val;
                        return true;
                    }
                    false
                } else if mode != SetMode::Replace {
                    pairs.push((k.clone(), val));
                    true
                } else {
                    false
                }
            }
            (Json::Arr(items), Step::Index(i)) => {
                if *i < items.len() {
                    if mode != SetMode::Insert {
                        items[*i] = val;
                        return true;
                    }
                    false
                } else if mode != SetMode::Replace {
                    items.push(val);
                    true
                } else {
                    false
                }
            }
            _ => false,
        };
    }
    let next = match (cur, &steps[0]) {
        (Json::Obj(pairs), Step::Key(k)) => pairs.iter_mut().find(|p| &p.0 == k).map(|p| &mut p.1),
        (Json::Arr(items), Step::Index(i)) => items.get_mut(*i),
        _ => None,
    };
    match next {
        Some(n) => set_recurse(n, &steps[1..], val, mode),
        None => false,
    }
}

fn remove_recurse(cur: &mut Json, steps: &[Step]) -> bool {
    if steps.len() == 1 {
        return match (cur, &steps[0]) {
            (Json::Obj(pairs), Step::Key(k)) => {
                if let Some(pos) = pairs.iter().position(|p| &p.0 == k) {
                    pairs.remove(pos);
                    true
                } else {
                    false
                }
            }
            (Json::Arr(items), Step::Index(i)) if *i < items.len() => {
                items.remove(*i);
                true
            }
            _ => false,
        };
    }
    let next = match (cur, &steps[0]) {
        (Json::Obj(pairs), Step::Key(k)) => pairs.iter_mut().find(|p| &p.0 == k).map(|p| &mut p.1),
        (Json::Arr(items), Step::Index(i)) => items.get_mut(*i),
        _ => None,
    };
    match next {
        Some(n) => remove_recurse(n, &steps[1..]),
        None => false,
    }
}

enum Step {
    Key(String),
    Index(usize),
}

/// Parse `$`, `$.a`, `$."a b"`, `$[0]`, `$.a[1].b` into navigation steps.
fn parse_path(path: &str) -> Option<Vec<Step>> {
    let b = path.as_bytes();
    if b.first() != Some(&b'$') {
        return None;
    }
    let mut p = 1;
    let mut steps = Vec::new();
    while p < b.len() {
        match b[p] {
            b'.' => {
                p += 1;
                if p < b.len() && b[p] == b'"' {
                    p += 1;
                    let start = p;
                    while p < b.len() && b[p] != b'"' {
                        p += 1;
                    }
                    steps.push(Step::Key(
                        String::from_utf8_lossy(&b[start..p]).into_owned(),
                    ));
                    p += 1; // closing quote
                } else {
                    let start = p;
                    while p < b.len() && b[p] != b'.' && b[p] != b'[' {
                        p += 1;
                    }
                    steps.push(Step::Key(
                        String::from_utf8_lossy(&b[start..p]).into_owned(),
                    ));
                }
            }
            b'[' => {
                p += 1;
                let start = p;
                while p < b.len() && b[p] != b']' {
                    p += 1;
                }
                let idx: usize = String::from_utf8_lossy(&b[start..p]).trim().parse().ok()?;
                steps.push(Step::Index(idx));
                p += 1; // closing bracket
            }
            _ => return None,
        }
    }
    Some(steps)
}

fn skip_ws(b: &[u8], p: &mut usize) {
    while *p < b.len() && matches!(b[*p], b' ' | b'\t' | b'\n' | b'\r') {
        *p += 1;
    }
}

fn parse_value(b: &[u8], p: &mut usize, depth: usize) -> Option<Json> {
    skip_ws(b, p);
    match *b.get(*p)? {
        b'{' => parse_object(b, p, depth),
        b'[' => parse_array(b, p, depth),
        b'"' => Some(Json::Str(parse_string(b, p)?)),
        b't' => lit(b, p, "true", Json::Bool(true)),
        b'f' => lit(b, p, "false", Json::Bool(false)),
        b'n' => lit(b, p, "null", Json::Null),
        _ => parse_number(b, p),
    }
}

fn lit(b: &[u8], p: &mut usize, s: &str, v: Json) -> Option<Json> {
    if b[*p..].starts_with(s.as_bytes()) {
        *p += s.len();
        Some(v)
    } else {
        None
    }
}

fn parse_string(b: &[u8], p: &mut usize) -> Option<String> {
    if b.get(*p) != Some(&b'"') {
        return None;
    }
    *p += 1;
    let mut out = String::new();
    while *p < b.len() {
        match b[*p] {
            b'"' => {
                *p += 1;
                return Some(out);
            }
            b'\\' => {
                *p += 1;
                match *b.get(*p)? {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'n' => out.push('\n'),
                    b't' => out.push('\t'),
                    b'r' => out.push('\r'),
                    b'b' => out.push('\u{8}'),
                    b'f' => out.push('\u{c}'),
                    b'u' => {
                        let unit = hex4(b, *p + 1)?;
                        *p += 4;
                        let cp = match unit {
                            // A UTF-16 surrogate pair: `\ud83d\ude00` is one
                            // character, and was refused as two halves.
                            0xD800..=0xDBFF => {
                                if b.get(*p + 1..*p + 3) != Some(b"\\u".as_slice()) {
                                    return None;
                                }
                                let low = hex4(b, *p + 3)?;
                                if !(0xDC00..=0xDFFF).contains(&low) {
                                    return None;
                                }
                                *p += 6;
                                0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00)
                            }
                            other => other,
                        };
                        out.push(char::from_u32(cp)?);
                    }
                    _ => return None,
                }
                *p += 1;
            }
            _ => {
                // Up to the next quote or backslash, as text. The bytes are a
                // `&str`'s, and those two are ASCII, so the run is whole UTF-8.
                // Copied one byte at a time, each byte of a character that is
                // not ASCII failed to decode on its own and was dropped:
                // "Blåbær" came out "Blbr", from JSON_EXTRACT and JSON_SET.
                let start = *p;
                while *p < b.len() && b[*p] != b'"' && b[*p] != b'\\' {
                    *p += 1;
                }
                out.push_str(std::str::from_utf8(&b[start..*p]).ok()?);
            }
        }
    }
    None
}

/// The four hex digits at `at` as a UTF-16 code unit.
fn hex4(b: &[u8], at: usize) -> Option<u32> {
    let hex = std::str::from_utf8(b.get(at..at + 4)?).ok()?;
    u32::from_str_radix(hex, 16).ok()
}

fn parse_number(b: &[u8], p: &mut usize) -> Option<Json> {
    let start = *p;
    while *p < b.len() && matches!(b[*p], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E') {
        *p += 1;
    }
    let s = std::str::from_utf8(&b[start..*p]).ok()?;
    if s.is_empty() || s.parse::<f64>().is_err() {
        return None;
    }
    Some(Json::Num(s.to_string()))
}

fn parse_array(b: &[u8], p: &mut usize, depth: usize) -> Option<Json> {
    if depth >= MAX_JSON_DEPTH {
        return None;
    }
    *p += 1;
    let mut items = Vec::new();
    skip_ws(b, p);
    if b.get(*p) == Some(&b']') {
        *p += 1;
        return Some(Json::Arr(items));
    }
    loop {
        items.push(parse_value(b, p, depth + 1)?);
        skip_ws(b, p);
        match b.get(*p)? {
            b',' => {
                *p += 1;
            }
            b']' => {
                *p += 1;
                return Some(Json::Arr(items));
            }
            _ => return None,
        }
    }
}

fn parse_object(b: &[u8], p: &mut usize, depth: usize) -> Option<Json> {
    if depth >= MAX_JSON_DEPTH {
        return None;
    }
    *p += 1;
    let mut pairs = Vec::new();
    skip_ws(b, p);
    if b.get(*p) == Some(&b'}') {
        *p += 1;
        return Some(Json::Obj(pairs));
    }
    loop {
        skip_ws(b, p);
        let key = parse_string(b, p)?;
        skip_ws(b, p);
        if b.get(*p) != Some(&b':') {
            return None;
        }
        *p += 1;
        let val = parse_value(b, p, depth + 1)?;
        pairs.push((key, val));
        skip_ws(b, p);
        match b.get(*p)? {
            b',' => {
                *p += 1;
            }
            b'}' => {
                *p += 1;
                return Some(Json::Obj(into_mysql_key_order(pairs)));
            }
            _ => return None,
        }
    }
}

/// A string as MySQL quotes it in JSON: `"` and `\\` escaped, the control
/// characters with a short escape as that (`\\b \\f \\n \\r \\t`), the rest
/// below U+0020 as `\\u00XX`, and everything else -- `/`, non-ASCII -- as is.
fn write_quoted(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            _ => out.push(c),
        }
    }
    out.push('"');
}

/// MySQL's order of an object's keys: shorter (in bytes) first, then by bytes.
fn mysql_key_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    a.len()
        .cmp(&b.len())
        .then_with(|| a.as_bytes().cmp(b.as_bytes()))
}

/// An object's members in the order MySQL stores and prints them (see
/// [`mysql_key_cmp`]); a duplicated key keeps its last value.
fn mysql_key_order(pairs: &[(String, Json)]) -> Vec<&(String, Json)> {
    let mut order: Vec<&(String, Json)> = pairs.iter().collect();
    // Stable, so of a duplicated key the last one written comes last.
    order.sort_by(|(a, _), (b, _)| mysql_key_cmp(a, b));
    let mut out: Vec<&(String, Json)> = Vec::with_capacity(order.len());
    for pair in order {
        match out.last_mut() {
            Some(last) if last.0 == pair.0 => *last = pair,
            _ => out.push(pair),
        }
    }
    out
}

/// [`mysql_key_order`] of a parsed object's own members.
fn into_mysql_key_order(mut pairs: Vec<(String, Json)>) -> Vec<(String, Json)> {
    pairs.sort_by(|(a, _), (b, _)| mysql_key_cmp(a, b));
    // `dedup_by` drops the later of two equal keys; give the kept one its value.
    pairs.dedup_by(|later, kept| {
        let same = later.0 == kept.0;
        if same {
            std::mem::swap(&mut later.1, &mut kept.1);
        }
        same
    });
    pairs
}

/// Two JSON numbers equal by value: as integers while both are, else as
/// doubles.
fn same_number(a: &str, b: &str) -> bool {
    let int = |s: &str| -> Option<i128> {
        if s.contains(['.', 'e', 'E']) {
            None
        } else {
            s.parse().ok()
        }
    };
    match (int(a), int(b)) {
        (Some(x), Some(y)) => x == y,
        _ => match (a.parse::<f64>(), b.parse::<f64>()) {
            (Ok(x), Ok(y)) => x == y,
            _ => a == b,
        },
    }
}

/// A JSON number as MySQL writes it. An integer that fits a signed or an
/// unsigned 64-bit one stays an integer (`-0` is `0`); anything else is a
/// double, written in the fewest digits that read back as it -- in fixed
/// notation while its decimal exponent is from -15 to 14 (or the digits run
/// past the point), with `.0` when it is whole, else as `1.5e16`. MySQL's
/// parser can land one unit in the last place away from the nearest double
/// on 16- and 17-digit input; this one does not.
pub fn canonical_number(text: &str) -> String {
    let integer = !text.contains(['.', 'e', 'E']);
    if integer {
        if let Ok(i) = text.parse::<i64>() {
            return i.to_string();
        }
        if let Ok(u) = text.parse::<u64>() {
            return u.to_string();
        }
    }
    match text.parse::<f64>() {
        Ok(d) if d.is_finite() => format_double(d),
        _ => text.to_string(),
    }
}

fn format_double(d: f64) -> String {
    // `{:e}` gives the shortest digits that round-trip: "1.25e-15", "-0e0".
    let sci = format!("{d:e}");
    let (mantissa, exp) = sci.split_once('e').expect("`{:e}` has an exponent");
    let exp: i32 = exp.parse().expect("`{:e}` exponent is an integer");
    let (sign, mantissa) = match mantissa.strip_prefix('-') {
        Some(m) => ("-", m),
        None => ("", mantissa),
    };
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let n = digits.len() as i32;
    // Where the decimal point falls, counted from the first digit.
    let point = exp + 1;
    let fixed = if point > 0 {
        point <= 15 || point < n
    } else {
        point >= -14
    };
    let mut out = String::from(sign);
    if !fixed {
        out.push_str(&digits[..1]);
        if n > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push_str(&format!("e{exp}"));
    } else if point <= 0 {
        out.push_str("0.");
        out.push_str(&"0".repeat((-point) as usize));
        out.push_str(&digits);
    } else if point < n {
        out.push_str(&digits[..point as usize]);
        out.push('.');
        out.push_str(&digits[point as usize..]);
    } else {
        out.push_str(&digits);
        out.push_str(&"0".repeat((point - n) as usize));
        out.push_str(".0");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_keep_text_that_is_not_ascii() {
        let doc =
            parse(r#"{"navn": "Blåbær", "ø": "æ\u00e5", "emoji": "😀 \ud83d\ude00"}"#).unwrap();
        assert_eq!(doc.extract("$.navn"), Some(Json::Str("Blåbær".into())));
        assert_eq!(doc.extract("$.ø"), Some(Json::Str("æå".into())));
        assert_eq!(doc.extract("$.emoji"), Some(Json::Str("😀 😀".into())));
        // MySQL orders keys by their length in bytes: "ø" is two.
        assert_eq!(doc.keys().unwrap(), vec!["ø", "navn", "emoji"]);
        assert!(doc.to_json_string().contains("\"Blåbær\""));
        // A surrogate half on its own is no character.
        assert!(parse(r#""\ud83d""#).is_none());
        assert!(parse(r#""\ud83d\u0041""#).is_none());
        assert!(parse(r#""\ude00""#).is_none());
    }

    #[test]
    fn canonical_is_mysqls_normal_form() {
        // Each checked against MySQL 8.4: the text stored into a JSON column
        // and read back.
        let cases = [
            (r#"{"b":1,"a":2}"#, r#"{"a": 2, "b": 1}"#),
            (
                r#"{"bb":1,"a":2,"c":3,"aa":4}"#,
                r#"{"a": 2, "c": 3, "aa": 4, "bb": 1}"#,
            ),
            (r#"{"a":1,"a":2}"#, r#"{"a": 2}"#),
            (
                r#"  { "x" : [ 1 , { "y" : null } ] }  "#,
                r#"{"x": [1, {"y": null}]}"#,
            ),
            (
                r#"{"Z":1,"a":2,"é":3,"B":4}"#,
                r#"{"B": 4, "Z": 1, "a": 2, "é": 3}"#,
            ),
            (
                "[1.0, 1.50, 1e2, 1E-2, -0, 0.1, 123456789012345678901234567890, \
                 18446744073709551615, 9223372036854775808, -9223372036854775809, \
                 3.0e0, 2.5e-7, 1e21]",
                "[1.0, 1.5, 100.0, 0.01, 0, 0.1, 1.2345678901234568e29, \
                 18446744073709551615, 9223372036854775808, -9.223372036854776e18, \
                 3.0, 0.00000025, 1e21]",
            ),
            (
                r#"["a\"b", "\u00e9", "\n", "\u0001", "\/", "\t", "\b", "\f", "\ud83d\ude00"]"#,
                r#"["a\"b", "é", "\n", "\u0001", "/", "\t", "\b", "\f", "😀"]"#,
            ),
            ("[[],{}]", "[[], {}]"),
            ("-1.25", "-1.25"),
            (r#""\\""#, r#""\\""#),
        ];
        for (text, mysql) in cases {
            assert_eq!(canonical(text).as_deref(), Some(mysql), "{text}");
            // The normal form is its own normal form.
            assert_eq!(canonical(mysql).as_deref(), Some(mysql), "{mysql}");
        }
        assert_eq!(canonical("[1,"), None);
    }

    #[test]
    fn a_json_value_keeps_its_numbers() {
        // A DECIMAL put in by a JSON function keeps its scale, as in MySQL,
        // when the value is read again; JSON text does not.
        let stored = Json::Arr(vec![Json::Num("1.50".into())]).to_json_string();
        assert_eq!(stored, "[1.50]");
        assert_eq!(parse(&stored).unwrap().to_json_string(), "[1.50]");
        assert_eq!(parse_text(&stored).unwrap().to_json_string(), "[1.5]");
    }

    #[test]
    fn duplicated_keys_keep_the_last_value_in_any_number() {
        let doc = parse(r#"{"b": 1, "a": 2, "b": 3, "a": 4, "b": 5}"#).unwrap();
        assert_eq!(doc.to_json_string(), r#"{"a": 4, "b": 5}"#);
        assert_eq!(doc.extract("$.b"), Some(Json::Num("5".into())));
        // Built, not parsed (`JSON_OBJECT('a', 1, 'a', 2)`): the same.
        let built = Json::Obj(vec![
            ("a".into(), Json::Num("1".into())),
            ("a".into(), Json::Num("2".into())),
        ]);
        assert_eq!(built.to_json_string(), r#"{"a": 2}"#);
        assert_eq!(built.keys().unwrap(), vec!["a"]);
        // Many keys, many duplicated, parse in time.
        let text = format!(
            "{{{}}}",
            (0..40_000)
                .map(|i| format!(r#""k{}": {i}"#, i % 20_000))
                .collect::<Vec<_>>()
                .join(",")
        );
        let doc = parse(&text).unwrap();
        assert_eq!(doc.length(), 20_000);
        assert_eq!(doc.extract("$.k7"), Some(Json::Num("20007".into())));
    }

    #[test]
    fn contains_compares_numbers_by_value() {
        let has = |t: &str, c: &str| parse(t).unwrap().contains(&parse(c).unwrap());
        assert!(has(r#"{"n": 1.0}"#, r#"{"n": 1}"#));
        assert!(has("[1.0, 2]", "1"));
        assert!(has("[1]", "1.0"));
        assert!(has("[1.50]", "1.5"));
        assert!(has("[18446744073709551615]", "18446744073709551615"));
        assert!(!has("[1]", "2"));
        assert!(!has("[1]", r#""1""#));
    }

    #[test]
    fn numbers_are_written_as_mysql_writes_a_double() {
        // From MySQL 8.4: `CAST('[1e15, ...]' AS JSON)`.
        for (text, mysql) in [
            ("1e14", "100000000000000.0"),
            ("9.99e14", "999000000000000.0"),
            ("1e15", "1e15"),
            ("1.25e15", "1.25e15"),
            ("1.5e16", "1.5e16"),
            ("123456789012345678.5", "1.2345678901234568e17"),
            ("123.456e3", "123456.0"),
            ("1e-5", "0.00001"),
            ("1e-14", "0.00000000000001"),
            ("1e-15", "0.000000000000001"),
            ("-2.5e-20", "-2.5e-20"),
            ("-0.0", "-0.0"),
            ("0e5", "0.0"),
        ] {
            assert_eq!(canonical_number(text), mysql, "{text}");
        }
    }

    #[test]
    fn rejects_pathologically_deep_nesting() {
        // A document nested far past the limit must be rejected (returns None),
        // not parsed recursively into a stack overflow. Uses an iterative build
        // + explicit checks so the test itself never recurses.
        let deep_arr = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
        assert!(parse(&deep_arr).is_none());
        let deep_obj = format!("{}true{}", "{\"a\":".repeat(100_000), "}".repeat(100_000));
        assert!(parse(&deep_obj).is_none());
        // Nesting right at the boundary is fine.
        let ok = format!(
            "{}1{}",
            "[".repeat(super::MAX_JSON_DEPTH),
            "]".repeat(super::MAX_JSON_DEPTH)
        );
        assert!(parse(&ok).is_some());
        // One past the boundary is rejected.
        let over = format!(
            "{}1{}",
            "[".repeat(super::MAX_JSON_DEPTH + 1),
            "]".repeat(super::MAX_JSON_DEPTH + 1)
        );
        assert!(parse(&over).is_none());
    }

    #[test]
    fn extract_paths() {
        let j = parse(r#"{"a": {"b": [10, 20, 30]}, "name": "x"}"#).unwrap();
        assert_eq!(j.extract("$.name").unwrap(), Json::Str("x".into()));
        assert_eq!(j.extract("$.a.b[1]").unwrap(), Json::Num("20".into()));
        assert_eq!(j.extract("$.a.b").unwrap().to_json_string(), "[10, 20, 30]");
        assert!(j.extract("$.missing").is_none());
    }

    #[test]
    fn unquote() {
        assert_eq!(parse(r#""hi""#).unwrap().to_unquoted(), "hi");
        assert_eq!(parse("42").unwrap().to_unquoted(), "42");
    }
}
