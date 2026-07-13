//! Shared harness for the ElyraSQL wire integration tests.
//!
//! Starts a real ElyraSQL server in-process on an ephemeral port and connects
//! to it with an *independent* MySQL driver (`mysql_async`), so the tests
//! exercise the actual wire protocol end to end -- handshake, auth, the text
//! protocol, prepared statements, result-set encoding -- exactly as a real
//! client would. Nothing here reaches into ElyraSQL internals to check results.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use elyra_engine::Engine;
use elyra_server::{serve, Auth, ServerConfig};
use elyra_storage::Db;

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A running test server. The data file is removed and the serving task is
/// aborted when this is dropped.
pub struct TestServer {
    pub port: u16,
    data_path: std::path::PathBuf,
    handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    /// Start a server with no authentication (open mode).
    pub async fn start() -> TestServer {
        Self::start_inner(Auth::open()).await
    }

    /// Start a server that requires `root`/`password` (Admin).
    pub async fn start_with_auth(user: &str, password: &str) -> TestServer {
        let auth = Auth::with_users(vec![(
            user.to_string(),
            password.to_string(),
            elyra_core::Privilege::Admin,
        )]);
        Self::start_inner(auth).await
    }

    async fn start_inner(auth: Auth) -> TestServer {
        // Reserve an ephemeral port from the OS, then hand the address to the
        // server. (Tiny TOCTOU window, acceptable for tests.)
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let data_path =
            std::env::temp_dir().join(format!("elyrasql-it-{}-{}.edb", std::process::id(), n));
        let _ = std::fs::remove_file(&data_path);

        let db = Db::open(&data_path).expect("open db");
        let auth = Arc::new(auth.with_db(db.clone()));
        let engine = Engine::new(db);

        let config = ServerConfig {
            listen: format!("127.0.0.1:{port}"),
            auth,
            tls: None,
            slow_query_ms: 0,
            metrics_listen: None,
            audit_log: None,
            replication_listen: None,
            read_only: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        let handle = tokio::spawn(async move {
            let _ = serve(config, engine).await;
        });

        // Wait until the listener accepts connections.
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        TestServer {
            port,
            data_path,
            handle,
        }
    }

    /// Connect a fresh `mysql_async` connection as `root` with no password.
    pub async fn conn(&self) -> mysql_async::Conn {
        self.conn_as("root", "").await
    }

    pub async fn conn_as(&self, user: &str, password: &str) -> mysql_async::Conn {
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(self.port)
            .user(Some(user))
            .pass(if password.is_empty() {
                None
            } else {
                Some(password)
            })
            .prefer_socket(false);
        mysql_async::Conn::new(opts).await.expect("connect")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
        let _ = std::fs::remove_file(&self.data_path);
    }
}
