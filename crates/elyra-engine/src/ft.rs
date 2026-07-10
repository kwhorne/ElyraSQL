//! Full-text tokenization and stemming, shared by the full-text index and
//! `MATCH ... AGAINST`.
//!
//! Stemming uses the Snowball algorithms (`rust-stemmers`) rather than ad-hoc
//! suffix stripping, so it is linguistically correct (e.g. `running` -> `run`,
//! but `string` stays `string` and `sing` stays `sing`) and supports many
//! languages. The language is chosen with `ELYRASQL_FULLTEXT_LANGUAGE`
//! (default `english`); set it to `none` to disable stemming (store raw
//! lowercased tokens), which suits languages Snowball doesn't cover.

use std::sync::OnceLock;

use rust_stemmers::{Algorithm, Stemmer};

/// Map the configured language name to a Snowball algorithm; `None` disables
/// stemming.
fn algorithm() -> Option<Algorithm> {
    let lang = std::env::var("ELYRASQL_FULLTEXT_LANGUAGE")
        .unwrap_or_else(|_| "english".into())
        .to_ascii_lowercase();
    Some(match lang.as_str() {
        "none" | "off" | "" => return None,
        "arabic" => Algorithm::Arabic,
        "danish" => Algorithm::Danish,
        "dutch" => Algorithm::Dutch,
        "finnish" => Algorithm::Finnish,
        "french" => Algorithm::French,
        "german" => Algorithm::German,
        "greek" => Algorithm::Greek,
        "hungarian" => Algorithm::Hungarian,
        "italian" => Algorithm::Italian,
        "norwegian" => Algorithm::Norwegian,
        "portuguese" => Algorithm::Portuguese,
        "romanian" => Algorithm::Romanian,
        "russian" => Algorithm::Russian,
        "spanish" => Algorithm::Spanish,
        "swedish" => Algorithm::Swedish,
        "tamil" => Algorithm::Tamil,
        "turkish" => Algorithm::Turkish,
        _ => Algorithm::English,
    })
}

/// The process-wide stemmer (built once; `None` when stemming is disabled).
fn stemmer() -> Option<&'static Stemmer> {
    static S: OnceLock<Option<Stemmer>> = OnceLock::new();
    S.get_or_init(|| algorithm().map(Stemmer::create)).as_ref()
}

/// Stem one word: lowercase, then apply the configured Snowball stemmer.
pub fn stem(word: &str) -> String {
    let w = word.to_lowercase();
    match stemmer() {
        Some(s) => s.stem(&w).into_owned(),
        None => w,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn english_stemming_is_linguistically_sound() {
        // Inflected forms collapse to a shared stem...
        assert_eq!(stem("running"), stem("run"));
        assert_eq!(stem("dogs"), stem("dog"));
        assert_eq!(stem("matched"), stem("match"));
        assert_eq!(stem("studies"), stem("study"));
        // ...while short/root words are NOT mangled the way the old ad-hoc
        // stripper did (string->str, greed->gre, speed->spe, sing->s).
        assert_eq!(stem("string"), "string");
        assert_eq!(stem("greed"), "greed");
        assert_eq!(stem("speed"), "speed");
        assert_eq!(stem("sing"), "sing");
        assert_eq!(stem("feed"), "feed");
    }

    #[test]
    fn tokenize_splits_and_stems() {
        assert_eq!(tokenize("The Running Dogs!"), vec!["the", "run", "dog"]);
    }
}
