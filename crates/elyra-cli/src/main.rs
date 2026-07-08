//! `elyrasql` — the ElyraSQL server binary.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use elyra_engine::Engine;
use elyra_storage::Db;

/// ElyraSQL — a robust, MySQL-compatible SQL server written in Rust.
#[derive(Parser)]
#[command(name = "elyrasql", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the ElyraSQL server.
    Serve {
        /// Path to the single ElyraSQL database file.
        #[arg(long, env = "ELYRASQL_DATA", default_value = "elyra.edb")]
        data: PathBuf,

        /// Address to bind the MySQL-compatible listener to.
        #[arg(long, env = "ELYRASQL_LISTEN", default_value = "127.0.0.1:3307")]
        listen: String,

        /// Require this username to connect (enables authentication).
        #[arg(long, env = "ELYRASQL_USER")]
        user: Option<String>,

        /// Password for --user.
        #[arg(long, env = "ELYRASQL_PASSWORD", default_value = "")]
        password: String,

        /// Additional user as `user:password:role` (role: admin|write|read).
        /// Repeatable. Enables authentication.
        #[arg(long = "auth", value_name = "USER:PASS:ROLE")]
        auth: Vec<String>,

        /// PEM certificate file to enable TLS.
        #[arg(long, env = "ELYRASQL_TLS_CERT", requires = "tls_key")]
        tls_cert: Option<PathBuf>,

        /// PEM private key file to enable TLS.
        #[arg(long, env = "ELYRASQL_TLS_KEY", requires = "tls_cert")]
        tls_key: Option<PathBuf>,
    },
    /// Print version and build information.
    Version,
}

/// Parse a `user:password:role` auth spec (role optional, defaults to admin).
fn parse_auth_spec(spec: &str) -> anyhow::Result<(String, String, elyra_core::Privilege)> {
    let parts: Vec<&str> = spec.splitn(3, ':').collect();
    if parts.len() < 2 {
        anyhow::bail!("--auth must be user:password[:role], got '{spec}'");
    }
    let role = match parts.get(2).copied().unwrap_or("admin").to_ascii_lowercase().as_str() {
        "admin" => elyra_core::Privilege::Admin,
        "write" | "readwrite" => elyra_core::Privilege::Write,
        "read" | "readonly" => elyra_core::Privilege::Read,
        other => anyhow::bail!("unknown role '{other}' (use admin|write|read)"),
    };
    Ok((parts[0].to_string(), parts[1].to_string(), role))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!("{} {}", elyra_core::PRODUCT_NAME, elyra_core::SERVER_VERSION);
        }
        Command::Serve { data, listen, user, password, auth, tls_cert, tls_key } => {
            tracing::info!(?data, "opening ElyraSQL database file");
            let db = Db::open(&data)?;
            let engine = Engine::new(db);

            let mut entries: Vec<(String, String, elyra_core::Privilege)> = Vec::new();
            if let Some(u) = user {
                entries.push((u, password, elyra_core::Privilege::Admin));
            }
            for spec in auth {
                entries.push(parse_auth_spec(&spec)?);
            }
            let auth = std::sync::Arc::new(if entries.is_empty() {
                elyra_server::Auth::open()
            } else {
                elyra_server::Auth::with_users(entries)
            });
            let tls = match (tls_cert, tls_key) {
                (Some(cert), Some(key)) => {
                    Some(std::sync::Arc::new(elyra_server::load_tls(&cert, &key)?))
                }
                _ => None,
            };

            let config = elyra_server::ServerConfig { listen, auth, tls };
            elyra_server::serve(config, engine).await?;
        }
    }
    Ok(())
}
