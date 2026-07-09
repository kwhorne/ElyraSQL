//! Full-text tokenization and light stemming, shared by the full-text index and
//! `MATCH ... AGAINST`.

/// A light English stemmer: lowercases and strips a few common suffixes so that
/// e.g. `dogs`/`dog`, `foxes`/`fox`, and `matched`/`match` collapse together.
/// Not a full Porter stemmer, but improves recall.
pub fn stem(word: &str) -> String {
    let w = word.to_lowercase();
    let n = w.len();
    if n > 5 && w.ends_with("ies") {
        return format!("{}y", &w[..n - 3]);
    }
    if n > 5 && w.ends_with("ing") {
        return w[..n - 3].to_string();
    }
    if n > 4 && w.ends_with("edly") {
        return w[..n - 4].to_string();
    }
    if n > 4 && w.ends_with("ly") {
        return w[..n - 2].to_string();
    }
    if n > 4 && w.ends_with("ed") {
        return w[..n - 2].to_string();
    }
    if n > 4 && w.ends_with("es") {
        return w[..n - 2].to_string();
    }
    if n > 3 && w.ends_with('s') && !w.ends_with("ss") {
        return w[..n - 1].to_string();
    }
    w
}

/// Tokenize text into stemmed terms (lowercased, split on non-alphanumerics).
pub fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(stem)
        .collect()
}

/// Unique stemmed terms of `text` (for building index entries).
pub fn unique_terms(text: &str) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for t in tokenize(text) {
        if seen.insert(t.clone()) {
            out.push(t);
        }
    }
    out
}
