//! `ai_embed('text')` — generate an embedding vector by calling an
//! OpenAI-compatible embeddings endpoint, so query vectors (and stored column
//! values) can be produced directly in SQL:
//!
//! ```sql
//! -- generate the query vector inline for hybrid search
//! SELECT id, HYBRID(body, 'privacy', embedding, ai_embed('privacy')) AS score
//! FROM docs ORDER BY score DESC LIMIT 10;
//!
//! -- populate embeddings on insert
//! INSERT INTO docs VALUES (1, 'some text', ai_embed('some text'));
//! ```
//!
//! Works with cloud providers (OpenAI, ...) and local servers (Ollama, LM
//! Studio, llama.cpp, vLLM) that expose `/v1/embeddings`. Configured via
//! `ELYRASQL_AI_EMBED_URL` (+ optional `_KEY`, `_MODEL`).
//!
//! `ai_embed('<constant>')` calls are resolved in an async pre-pass: each unique
//! text is embedded once (cached) and the call is rewritten to the vector as a
//! string literal, which `VEC_DISTANCE` / `HYBRID` / `VECTOR` columns parse.
//! Only constant-string arguments are supported (per-row `ai_embed(col)` is not).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use elyra_core::{Error, Result};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, SelectItem, SetExpr, Statement,
    Value as SqlValue,
};

fn config() -> Option<(String, Option<String>, String)> {
    let url = std::env::var("ELYRASQL_AI_EMBED_URL")
        .ok()
        .filter(|s| !s.is_empty())?;
    let key = std::env::var("ELYRASQL_AI_EMBED_KEY")
        .ok()
        .filter(|s| !s.is_empty());
    let model = std::env::var("ELYRASQL_AI_EMBED_MODEL")
        .unwrap_or_else(|_| "text-embedding-3-small".into());
    Some((url, key, model))
}

fn cache() -> &'static Mutex<HashMap<String, Vec<f32>>> {
    static C: OnceLock<Mutex<HashMap<String, Vec<f32>>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Embed one text into a vector, cached per (model, text).
pub async fn embed(text: &str) -> Result<Vec<f32>> {
    let (url, key, model) = config().ok_or_else(|| {
        Error::Query("ai_embed: set ELYRASQL_AI_EMBED_URL (and optionally _KEY, _MODEL)".into())
    })?;
    let ckey = format!("{model}\u{0}{text}");
    if let Some(v) = cache().lock().unwrap().get(&ckey) {
        return Ok(v.clone());
    }
    let input = text.to_string();
    let model_c = model.clone();
    // ureq is blocking; keep it off the async runtime.
    let v = tokio::task::spawn_blocking(move || -> Result<Vec<f32>> {
        let body = serde_json::json!({ "input": input, "model": model_c });
        let mut req = ureq::post(&url).set("Content-Type", "application/json");
        if let Some(k) = &key {
            req = req.set("Authorization", &format!("Bearer {k}"));
        }
        let resp = req
            .send_json(body)
            .map_err(|e| Error::Query(format!("ai_embed request failed: {e}")))?;
        let json: serde_json::Value = resp
            .into_json()
            .map_err(|e| Error::Query(format!("ai_embed bad response: {e}")))?;
        let arr = json["data"][0]["embedding"]
            .as_array()
            .ok_or_else(|| Error::Query("ai_embed: no embedding in provider response".into()))?;
        Ok(arr
            .iter()
            .filter_map(|x| x.as_f64().map(|f| f as f32))
            .collect())
    })
    .await
    .map_err(|e| Error::Query(format!("ai_embed task failed: {e}")))??;
    cache().lock().unwrap().insert(ckey, v.clone());
    Ok(v)
}

/// Resolve every `ai_embed('<constant>')` in a statement: embed each unique
/// text once and rewrite the call to the vector as a string literal. No-op when
/// none are present.
pub async fn resolve_stmt(stmt: &mut Statement) -> Result<()> {
    let mut texts = Vec::new();
    collect_stmt(stmt, &mut texts);
    if texts.is_empty() {
        return Ok(());
    }
    texts.sort();
    texts.dedup();
    let mut map: HashMap<String, String> = HashMap::new();
    for t in texts {
        let v = embed(&t).await?;
        let lit = format!(
            "[{}]",
            v.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        map.insert(t, lit);
    }
    subst_stmt(stmt, &map);
    Ok(())
}

/// If `e` is `ai_embed('<constant>')`, return the constant text.
fn ai_embed_text(e: &Expr) -> Option<String> {
    let Expr::Function(f) = e else { return None };
    if !f.name.0.last()?.value.eq_ignore_ascii_case("ai_embed") {
        return None;
    }
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    if list.args.len() != 1 {
        return None;
    }
    let arg = match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => x,
        _ => return None,
    };
    match arg {
        Expr::Value(SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s)) => {
            Some(s.clone())
        }
        _ => None,
    }
}

pub(crate) fn fn_arg_exprs_mut(f: &mut sqlparser::ast::Function) -> Vec<&mut Expr> {
    let FunctionArguments::List(list) = &mut f.args else {
        return Vec::new();
    };
    list.args
        .iter_mut()
        .filter_map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => Some(x),
            FunctionArg::Named {
                arg: FunctionArgExpr::Expr(x),
                ..
            } => Some(x),
            _ => None,
        })
        .collect()
}

fn collect_expr(e: &Expr, out: &mut Vec<String>) {
    if let Some(t) = ai_embed_text(e) {
        out.push(t);
        return;
    }
    match e {
        Expr::Function(f) => {
            if let FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(x),
                        ..
                    } = a
                    {
                        collect_expr(x, out);
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr(left, out);
            collect_expr(right, out);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => collect_expr(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr(expr, out);
            collect_expr(low, out);
            collect_expr(high, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_expr(expr, out);
            for x in list {
                collect_expr(x, out);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(o) = operand {
                collect_expr(o, out);
            }
            for c in conditions {
                collect_expr(c, out);
            }
            for r in results {
                collect_expr(r, out);
            }
            if let Some(er) = else_result {
                collect_expr(er, out);
            }
        }
        _ => {}
    }
}

fn subst_expr(e: &mut Expr, map: &HashMap<String, String>) {
    if let Some(t) = ai_embed_text(e) {
        if let Some(lit) = map.get(&t) {
            *e = Expr::Value(SqlValue::SingleQuotedString(lit.clone()));
        }
        return;
    }
    match e {
        Expr::Function(f) => {
            for x in fn_arg_exprs_mut(f) {
                subst_expr(x, map);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            subst_expr(left, map);
            subst_expr(right, map);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => subst_expr(expr, map),
        Expr::Between {
            expr, low, high, ..
        } => {
            subst_expr(expr, map);
            subst_expr(low, map);
            subst_expr(high, map);
        }
        Expr::InList { expr, list, .. } => {
            subst_expr(expr, map);
            for x in list {
                subst_expr(x, map);
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(o) = operand {
                subst_expr(o, map);
            }
            for c in conditions {
                subst_expr(c, map);
            }
            for r in results {
                subst_expr(r, map);
            }
            if let Some(er) = else_result {
                subst_expr(er, map);
            }
        }
        _ => {}
    }
}

fn collect_query(q: &Query, out: &mut Vec<String>) {
    match q.body.as_ref() {
        SetExpr::Select(sel) => {
            for item in &sel.projection {
                match item {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        collect_expr(e, out)
                    }
                    _ => {}
                }
            }
            if let Some(w) = &sel.selection {
                collect_expr(w, out);
            }
        }
        SetExpr::Values(v) => {
            for row in &v.rows {
                for e in row {
                    collect_expr(e, out);
                }
            }
        }
        _ => {}
    }
    if let Some(ob) = &q.order_by {
        for o in &ob.exprs {
            collect_expr(&o.expr, out);
        }
    }
}

fn subst_query(q: &mut Query, map: &HashMap<String, String>) {
    match q.body.as_mut() {
        SetExpr::Select(sel) => {
            for item in &mut sel.projection {
                match item {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        subst_expr(e, map)
                    }
                    _ => {}
                }
            }
            if let Some(w) = &mut sel.selection {
                subst_expr(w, map);
            }
        }
        SetExpr::Values(v) => {
            for row in &mut v.rows {
                for e in row {
                    subst_expr(e, map);
                }
            }
        }
        _ => {}
    }
    if let Some(ob) = &mut q.order_by {
        for o in &mut ob.exprs {
            subst_expr(&mut o.expr, map);
        }
    }
}

fn collect_stmt(stmt: &Statement, out: &mut Vec<String>) {
    match stmt {
        Statement::Query(q) => collect_query(q, out),
        Statement::Insert(ins) => {
            if let Some(src) = &ins.source {
                collect_query(src, out);
            }
        }
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            for a in assignments {
                collect_expr(&a.value, out);
            }
            if let Some(w) = selection {
                collect_expr(w, out);
            }
        }
        _ => {}
    }
}

fn subst_stmt(stmt: &mut Statement, map: &HashMap<String, String>) {
    match stmt {
        Statement::Query(q) => subst_query(q, map),
        Statement::Insert(ins) => {
            if let Some(src) = &mut ins.source {
                subst_query(src, map);
            }
        }
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            for a in assignments {
                subst_expr(&mut a.value, map);
            }
            if let Some(w) = selection {
                subst_expr(w, map);
            }
        }
        _ => {}
    }
}
