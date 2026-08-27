//! The embeddings HTTP path, against a server that actually speaks it.
//!
//! Every other test of this feature injects a fake embedder, which covers the
//! sweep logic but never touches `ureq`, the request body, the `Authorization`
//! header or the response parsing. This exercises all of it end to end: a real
//! socket, a real HTTP exchange, and the vector landing in the row.
//!
//! The server here is deliberately minimal rather than a mock framework — it
//! asserts the request ElyraSQL sends is the one an OpenAI-compatible provider
//! expects, which is the part worth pinning.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;

use elyra_core::Privilege;
use elyra_engine::Engine;

/// What the fake provider saw, so the test can assert on the request too.
struct Captured {
    path: String,
    authorization: Option<String>,
    body: serde_json::Value,
}

/// Serve `count` embedding requests, then stop. Returns the listening address
/// and a receiver of what each request contained.
fn serve(count: usize, dimension: usize) -> (String, mpsc::Receiver<Captured>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = mpsc::channel();

    std::thread::spawn(move || {
        for _ in 0..count {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            // Read headers, then exactly Content-Length bytes of body.
            let mut raw = Vec::new();
            let mut buf = [0u8; 1024];
            let head_end = loop {
                let n = match stream.read(&mut buf) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                raw.extend_from_slice(&buf[..n]);
                if let Some(p) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                    break p + 4;
                }
            };
            let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
            let length: usize = head
                .lines()
                .find_map(|l| {
                    let (name, value) = l.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().ok())?
                })
                .unwrap_or(0);
            while raw.len() < head_end + length {
                let n = match stream.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                raw.extend_from_slice(&buf[..n]);
            }

            let path = head
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("")
                .to_string();
            let authorization = head.lines().find_map(|l| {
                let (name, value) = l.split_once(':')?;
                name.eq_ignore_ascii_case("authorization")
                    .then(|| value.trim().to_string())
            });
            let body: serde_json::Value =
                serde_json::from_slice(&raw[head_end..head_end + length]).unwrap_or_default();
            let _ = tx.send(Captured {
                path,
                authorization,
                body,
            });

            let vector: Vec<f32> = (0..dimension).map(|i| i as f32 / 100.0).collect();
            let payload = serde_json::json!({ "data": [ { "embedding": vector } ] });
            let payload = payload.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                payload.len(),
                payload
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });

    (format!("http://{addr}/v1/embeddings"), rx)
}

/// Set the provider environment for this process. The variables are global, so
/// this file holds exactly one test.
fn configure(url: &str, key: Option<&str>) {
    // SAFETY: single-threaded setup, before any embedding call in this process.
    unsafe {
        std::env::set_var("ELYRASQL_AI_EMBED_URL", url);
        match key {
            Some(k) => std::env::set_var("ELYRASQL_AI_EMBED_KEY", k),
            None => std::env::remove_var("ELYRASQL_AI_EMBED_KEY"),
        }
        std::env::set_var("ELYRASQL_AI_EMBED_MODEL", "server-default-model");
    }
}

#[tokio::test]
async fn a_sweep_talks_to_a_real_embeddings_endpoint() {
    let (url, requests) = serve(2, 4);
    configure(&url, Some("secret-key"));

    let engine = Engine::new(elyra_storage::Db::in_memory().unwrap());
    let session = engine.session();

    engine
        .execute(
            "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, embedding VECTOR(4))",
            Privilege::Admin,
            &session,
        )
        .await
        .unwrap();
    engine
        .execute(
            "CREATE EMBEDDING INDEX body_ix ON docs(body) INTO embedding \
             USING MODEL 'index-pinned-model'",
            Privilege::Admin,
            &session,
        )
        .await
        .unwrap();
    engine
        .execute(
            "INSERT INTO docs (id, body) VALUES (1, 'privacy law'), (2, 'tax law')",
            Privilege::Admin,
            &session,
        )
        .await
        .unwrap();

    let report = engine
        .sweep_embeddings()
        .await
        .unwrap()
        .expect("a provider is configured");
    assert_eq!(report.embedded, 2, "report: {report:?}");
    assert_eq!(report.failed, 0);

    // The request is the one an OpenAI-compatible provider expects.
    let first = requests
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(first.path, "/v1/embeddings");
    assert_eq!(first.authorization.as_deref(), Some("Bearer secret-key"));
    assert_eq!(
        first.body["model"], "index-pinned-model",
        "the index's model must win over ELYRASQL_AI_EMBED_MODEL"
    );
    let sent: Vec<String> = std::iter::once(first)
        .chain(requests.recv_timeout(std::time::Duration::from_secs(5)))
        .map(|c| c.body["input"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(sent.contains(&"privacy law".to_string()));
    assert!(sent.contains(&"tax law".to_string()));

    // And the vectors reached the rows.
    let rows = collect(&engine, &session, "SELECT embedding FROM docs ORDER BY id").await;
    let expected = elyra_core::Value::Vector(vec![0.0, 0.01, 0.02, 0.03]);
    assert_eq!(rows[0][0], expected);
    assert_eq!(rows[1][0], expected);

    // A second sweep is free: the recorded hashes already match, so the
    // endpoint is never contacted again. (It has stopped listening by now, so
    // any request would fail the sweep outright.)
    let again = engine.sweep_embeddings().await.unwrap().unwrap();
    assert_eq!(again.embedded, 0);
    assert_eq!(again.failed, 0);
}

async fn collect(
    engine: &Engine,
    session: &elyra_engine::Session,
    sql: &str,
) -> Vec<Vec<elyra_core::Value>> {
    let mut results = engine
        .execute(sql, Privilege::Admin, session)
        .await
        .unwrap();
    match results.remove(0) {
        elyra_engine::QueryResult::Rows(mut stream) => {
            let mut out = Vec::new();
            loop {
                let batch = stream.next_batch(256).await.unwrap();
                if batch.is_empty() {
                    break;
                }
                out.extend(batch);
            }
            out
        }
        _ => panic!("expected rows"),
    }
}
