//! `elyrasql` — the ElyraSQL server binary.

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use elyra_engine::Engine;
use elyra_storage::Storage;

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
        Command::Serve { data, listen } => {
            tracing::info!(?data, "opening ElyraSQL database file");
            let storage = Arc::new(Storage::open(&data)?);
            let engine = Engine::new(storage);
            elyra_server::serve(&listen, engine).await?;
        }
    }
    Ok(())
}
