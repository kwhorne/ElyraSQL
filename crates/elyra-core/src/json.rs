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

/// Parse a JSON document. Returns `None` if it is not valid JSON.
pub fn parse(s: &str) -> Option<Json> {
    let b = s.as_bytes();
    let mut p = 0;
    skip_ws(b, &mut p);
    let v = parse_value(b, &mut p)?;
    skip_ws(b, &mut p);
    if p == b.len() {
        Some(v)
    } else {
        None
    }
}

impl Json {
    /// Canonical JSON text (strings quoted).
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
                for (i, (k, v)) in pairs.iter().enumerate() {
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

fn parse_value(b: &[u8], p: &mut usize) -> Option<Json> {
    skip_ws(b, p);
    match *b.get(*p)? {
        b'{' => parse_object(b, p),
        b'[' => parse_array(b, p),
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
                        let hex = std::str::from_utf8(b.get(*p + 1..*p + 5)?).ok()?;
                        let cp = u32::from_str_radix(hex, 16).ok()?;
                        out.push(char::from_u32(cp)?);
                        *p += 4;
                    }
                    _ => return None,
                }
                *p += 1;
            }
            _ => {
                // Copy one UTF-8 byte at a time (valid because input is &str).
                let start = *p;
                *p += 1;
                out.push_str(std::str::from_utf8(&b[start..*p]).unwrap_or(""));
            }
        }
    }
    None
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

fn parse_array(b: &[u8], p: &mut usize) -> Option<Json> {
    *p += 1;
    let mut items = Vec::new();
    skip_ws(b, p);
    if b.get(*p) == Some(&b']') {
        *p += 1;
        return Some(Json::Arr(items));
    }
    loop {
        items.push(parse_value(b, p)?);
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

fn parse_object(b: &[u8], p: &mut usize) -> Option<Json> {
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
        let val = parse_value(b, p)?;
        pairs.push((key, val));
        skip_ws(b, p);
        match b.get(*p)? {
            b',' => {
                *p += 1;
            }
            b'}' => {
                *p += 1;
                return Some(Json::Obj(pairs));
            }
            _ => return None,
        }
    }
}

fn write_quoted(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

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
