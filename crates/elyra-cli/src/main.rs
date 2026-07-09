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

        /// Log queries taking at least this many milliseconds (0 disables).
        #[arg(long, env = "ELYRASQL_SLOW_QUERY_MS", default_value_t = 0)]
        slow_query_ms: u128,
    },
    /// Back up a database file to a new file (offline; the server must not be
    /// running against --data). For hot backups while serving, use the SQL
    /// command `BACKUP TO '<path>'` instead.
    Backup {
        /// Path to the source ElyraSQL database file.
        #[arg(long, env = "ELYRASQL_DATA", default_value = "elyra.edb")]
        data: PathBuf,
        /// Destination file (must not already exist).
        #[arg(long)]
        out: PathBuf,
    },
    /// Restore a database file from a backup (offline). Copies the backup into
    /// place; refuses to overwrite an existing target unless --force.
    Restore {
        /// Backup file to restore from.
        #[arg(long)]
        input: PathBuf,
        /// Target ElyraSQL database file to write.
        #[arg(long, env = "ELYRASQL_DATA", default_value = "elyra.edb")]
        data: PathBuf,
        /// Overwrite the target if it already exists.
        #[arg(long)]
        force: bool,
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
    let role = match parts
        .get(2)
        .copied()
        .unwrap_or("admin")
        .to_ascii_lowercase()
        .as_str()
    {
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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!(
                "{} {}",
                elyra_core::PRODUCT_NAME,
                elyra_core::SERVER_VERSION
            );
        }
        Command::Backup { data, out } => {
            if out.exists() {
                anyhow::bail!("backup target already exists: {}", out.display());
            }
            let db = Db::open(&data)?;
            let n = db.backup_to(out.clone()).await?;
            println!("backed up {n} rows to {}", out.display());
        }
        Command::Restore { input, data, force } => {
            if !input.exists() {
                anyhow::bail!("backup file not found: {}", input.display());
            }
            if data.exists() && !force {
                anyhow::bail!(
                    "target {} already exists (use --force to overwrite)",
                    data.display()
                );
            }
            // Validate that the backup opens as an ElyraSQL database before we
            // put it in place.
            Db::open(&input)?;
            std::fs::copy(&input, &data)?;
            println!("restored {} -> {}", input.display(), data.display());
        }
        Command::Serve {
            data,
            listen,
            user,
            password,
            auth,
            tls_cert,
            tls_key,
            slow_query_ms,
        } => {
            tracing::info!(?data, "opening ElyraSQL database file");
            let db = Db::open(&data)?;
            let engine = Engine::new(db.clone());

            let mut entries: Vec<(String, String, elyra_core::Privilege)> = Vec::new();
            if let Some(u) = user {
                entries.push((u, password, elyra_core::Privilege::Admin));
            }
            for spec in auth {
                entries.push(parse_auth_spec(&spec)?);
            }
            let auth = std::sync::Arc::new(
                if entries.is_empty() {
                    elyra_server::Auth::open()
                } else {
                    elyra_server::Auth::with_users(entries)
                }
                .with_db(db.clone()),
            );
            let tls = match (tls_cert, tls_key) {
                (Some(cert), Some(key)) => {
                    Some(std::sync::Arc::new(elyra_server::load_tls(&cert, &key)?))
                }
                _ => None,
            };

            let config = elyra_server::ServerConfig {
                listen,
                auth,
                tls,
                slow_query_ms,
            };
            elyra_server::serve(config, engine).await?;
        }
    }
    Ok(())
}
