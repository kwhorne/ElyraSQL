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

        /// Serve Prometheus metrics at http://<addr>/metrics (e.g. 0.0.0.0:9090).
        #[arg(long, env = "ELYRASQL_METRICS_LISTEN")]
        metrics_listen: Option<String>,

        /// Append every executed statement to this audit log file.
        #[arg(long, env = "ELYRASQL_AUDIT_LOG")]
        audit_log: Option<PathBuf>,

        /// Serve the replication stream at this address (makes this a primary).
        #[arg(long, env = "ELYRASQL_REPLICATION_LISTEN")]
        replication_listen: Option<String>,

        /// Append every committed write-set to this binlog for point-in-time
        /// recovery.
        #[arg(long, env = "ELYRASQL_BINLOG")]
        binlog: Option<PathBuf>,

        /// Semi-synchronous replication: wait up to this many ms for a replica to
        /// acknowledge each commit before returning (0 = asynchronous).
        #[arg(long, env = "ELYRASQL_SEMI_SYNC_MS", default_value_t = 0)]
        semi_sync_ms: u64,

        /// Quorum/synchronous replication: number of replica acks each commit
        /// must collect before returning (0 = asynchronous). Overrides
        /// --semi-sync-ms's implicit count of 1 when set.
        #[arg(long, env = "ELYRASQL_SYNC_REPLICAS", default_value_t = 0)]
        sync_replicas: u64,

        /// Strict sync: on timeout, fail the commit-confirmation instead of
        /// silently degrading to asynchronous (no silent data-loss window).
        #[arg(long, env = "ELYRASQL_SYNC_STRICT", default_value_t = false)]
        sync_strict: bool,
    },
    /// Replay a binlog onto a database for point-in-time recovery. Apply onto a
    /// restored backup (or an empty file) up to a target LSN or timestamp.
    BinlogReplay {
        /// Target database file (a restored backup, or a fresh file).
        #[arg(long, env = "ELYRASQL_DATA", default_value = "elyra.edb")]
        data: PathBuf,
        /// Binlog file to replay.
        #[arg(long)]
        binlog: PathBuf,
        /// Stop after this LSN (inclusive).
        #[arg(long)]
        until_lsn: Option<u64>,
        /// Stop at this Unix timestamp in milliseconds (inclusive).
        #[arg(long)]
        until_time_ms: Option<u64>,
    },
    /// Add or remove a cluster member at runtime. Send to the current leader
    /// (it propagates membership to followers via heartbeats). Start a new node
    /// before adding it so it can be reached.
    ClusterCtl {
        /// Control-plane address of a running node (preferably the leader).
        #[arg(long)]
        node: String,
        /// `add` or `remove`.
        #[arg(long)]
        action: String,
        /// The peer to add/remove, as `id@host:port` (remove only needs the id).
        #[arg(long)]
        peer: String,
    },
    /// Run as a cluster node with automatic failover (Raft-style leader
    /// election). The elected leader accepts writes; followers are read-only and
    /// replicate from the leader.
    Cluster {
        /// This node's numeric id (unique in the cluster).
        #[arg(long)]
        id: u64,
        /// Local database file.
        #[arg(long, env = "ELYRASQL_DATA", default_value = "elyra.edb")]
        data: PathBuf,
        /// MySQL listener address.
        #[arg(long, env = "ELYRASQL_LISTEN", default_value = "127.0.0.1:3307")]
        listen: String,
        /// Control-plane (election) listen address.
        #[arg(long)]
        control_listen: String,
        /// Replication endpoint address (advertised to followers).
        #[arg(long)]
        replication_listen: String,
        /// Peer nodes as `id@host:port` (control addresses). Repeatable.
        #[arg(long = "peer", value_name = "ID@HOST:PORT")]
        peers: Vec<String>,
    },
    /// Run as a read-only replica of a primary. The --data file is disposable:
    /// it is recreated and re-bootstrapped from the primary on start.
    Replica {
        /// Address of the primary's replication endpoint.
        #[arg(long, env = "ELYRASQL_PRIMARY")]
        primary: String,
        /// Local database file (recreated on start).
        #[arg(long, env = "ELYRASQL_DATA", default_value = "elyra-replica.edb")]
        data: PathBuf,
        /// Address to bind the (read-only) MySQL listener to.
        #[arg(long, env = "ELYRASQL_LISTEN", default_value = "127.0.0.1:3307")]
        listen: String,
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

fn main() -> anyhow::Result<()> {
    // A generous worker-thread stack gives headroom for deep (but depth-guarded)
    // recursion via triggers and stored procedures.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 * 1024 * 1024)
        .build()?;
    runtime.block_on(run())
}

async fn run() -> anyhow::Result<()> {
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
            metrics_listen,
            audit_log,
            replication_listen,
            binlog,
            semi_sync_ms,
            sync_replicas,
            sync_strict,
        } => {
            tracing::info!(?data, "opening ElyraSQL database file");
            if binlog.is_some() {
                tracing::info!(?binlog, "binlog (point-in-time recovery) enabled");
            }
            let db = Db::open_with_binlog(&data, binlog)?;
            if sync_replicas > 0 {
                tracing::info!(
                    sync_replicas,
                    timeout_ms = semi_sync_ms,
                    strict = sync_strict,
                    "quorum/synchronous replication enabled"
                );
                db.set_sync_policy(sync_replicas, semi_sync_ms, sync_strict);
            } else if semi_sync_ms > 0 {
                tracing::info!(
                    semi_sync_ms,
                    strict = sync_strict,
                    "semi-synchronous replication enabled"
                );
                db.set_sync_policy(1, semi_sync_ms, sync_strict);
            }
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
                metrics_listen,
                audit_log,
                replication_listen,
                read_only: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            };
            elyra_server::serve(config, engine).await?;
        }
        Command::BinlogReplay {
            data,
            binlog,
            until_lsn,
            until_time_ms,
        } => {
            let db = Db::open(&data)?;
            let n = elyra_storage::binlog::replay(&binlog, &db, until_lsn, until_time_ms).await?;
            println!("replayed {n} write-sets into {}", data.display());
        }
        Command::ClusterCtl { node, action, peer } => {
            let add = match action.to_ascii_lowercase().as_str() {
                "add" => true,
                "remove" => false,
                _ => return Err(anyhow::anyhow!("action must be 'add' or 'remove'")),
            };
            let p = elyra_server::cluster::parse_peer(&peer)?;
            elyra_server::cluster::send_membership(&node, add, p.id, p.control_addr).await?;
            println!("membership {action} acknowledged by {node}");
        }
        Command::Cluster {
            id,
            data,
            listen,
            control_listen,
            replication_listen,
            peers,
        } => {
            let db = Db::open(&data)?;
            let engine = Engine::new(db.clone());
            let read_only = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            let peers = peers
                .iter()
                .map(|p| elyra_server::cluster::parse_peer(p))
                .collect::<std::io::Result<Vec<_>>>()?;
            let node = elyra_server::cluster::Node::new(
                elyra_server::cluster::ClusterConfig {
                    id,
                    control_listen,
                    replication_addr: replication_listen.clone(),
                    peers,
                    state_path: Some(data.with_extension("raftstate")),
                    log_path: Some(data.with_extension("raftlog")),
                },
                db.clone(),
            );
            // Route all writes through the Raft log (leader proposes; commit +
            // apply before the client is acknowledged).
            db.set_consensus(node.clone());
            tokio::spawn(node.clone().run());
            tokio::spawn(elyra_server::cluster::follow_leadership(
                node.clone(),
                read_only.clone(),
            ));
            let auth = std::sync::Arc::new(elyra_server::Auth::open().with_db(db));
            let config = elyra_server::ServerConfig {
                listen,
                auth,
                tls: None,
                slow_query_ms: 0,
                metrics_listen: None,
                audit_log: None,
                replication_listen: Some(replication_listen),
                read_only,
            };
            elyra_server::serve(config, engine).await?;
        }
        Command::Replica {
            primary,
            data,
            listen,
        } => {
            // Fresh local file: a replica re-bootstraps its whole state.
            let _ = std::fs::remove_file(&data);
            tracing::info!(?data, %primary, "starting ElyraSQL replica");
            let db = Db::open(&data)?;
            let engine = Engine::new(db.clone());

            // Apply the primary's stream in the background; exit if it ends.
            let rdb = db.clone();
            tokio::spawn(async move {
                if let Err(e) = elyra_server::run_replica(primary, rdb).await {
                    tracing::error!(error = %e, "replication stopped; exiting for restart");
                    std::process::exit(1);
                }
            });

            let auth = std::sync::Arc::new(elyra_server::Auth::open().with_db(db));
            let config = elyra_server::ServerConfig {
                listen,
                auth,
                tls: None,
                slow_query_ms: 0,
                metrics_listen: None,
                audit_log: None,
                replication_listen: None,
                read_only: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            };
            elyra_server::serve(config, engine).await?;
        }
    }
    Ok(())
}
