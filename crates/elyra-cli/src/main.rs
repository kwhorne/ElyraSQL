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
        Command::Serve { data, listen, user, password, tls_cert, tls_key } => {
            tracing::info!(?data, "opening ElyraSQL database file");
            let db = Db::open(&data)?;
            let engine = Engine::new(db);

            let auth = match user {
                Some(u) => std::sync::Arc::new(elyra_server::Auth::single(&u, &password)),
                None => std::sync::Arc::new(elyra_server::Auth::open()),
            };
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
