//! Asynchronous primary → replica replication.
//!
//! The primary streams committed write-sets (tagged with a log sequence number)
//! to connected replicas. A replica first receives a consistent snapshot of the
//! whole keyspace, then applies the ongoing write stream in LSN order. Because
//! every write-set is an absolute key/value change, applying them in order is
//! idempotent, so the replica converges to the primary's exact state.
//!
//! This provides warm standbys and read scaling. Failover is manual: a replica's
//! data file is a complete ElyraSQL database, so promoting it is just pointing
//! clients at it (start it as a normal `serve`).

use std::io::{Error, ErrorKind};

use std::sync::Arc;

use elyra_storage::{Db, WriteEvent};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast::error::RecvError;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{info, warn};

/// Server-side TLS for the replication endpoint, from `ELYRASQL_CLUSTER_TLS_CERT`
/// + `ELYRASQL_CLUSTER_TLS_KEY`. `None` (plaintext) only when neither is set.
///
/// Partial or invalid TLS configuration fails closed.
pub(crate) fn cluster_server_tls() -> std::io::Result<Option<Arc<ServerConfig>>> {
    let cert = std::env::var("ELYRASQL_CLUSTER_TLS_CERT");
    let key = std::env::var("ELYRASQL_CLUSTER_TLS_KEY");
    match (cert, key) {
        (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => Ok(None),
        (Ok(cert), Ok(key)) => crate::load_tls(&cert, &key).map(|cfg| Some(Arc::new(cfg))),
        (cert, key) => Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "cluster TLS requires both ELYRASQL_CLUSTER_TLS_CERT and \
                 ELYRASQL_CLUSTER_TLS_KEY (cert: {}, key: {})",
                env_status(&cert),
                env_status(&key)
            ),
        )),
    }
}

fn env_status(value: &Result<String, std::env::VarError>) -> &'static str {
    match value {
        Ok(_) => "set",
        Err(std::env::VarError::NotPresent) => "unset",
        Err(std::env::VarError::NotUnicode(_)) => "non-Unicode",
    }
}

/// Client-side TLS for connecting to the primary, from `ELYRASQL_CLUSTER_TLS_CA`
/// (roots that must verify the primary's certificate) plus an optional
/// `ELYRASQL_CLUSTER_TLS_SERVER_NAME` (defaults to `localhost`). The primary's
/// certificate IS verified — there is no accept-any-cert mode.
pub(crate) fn cluster_client_tls() -> std::io::Result<Option<(TlsConnector, ServerName<'static>)>> {
    let Some(ca) = std::env::var("ELYRASQL_CLUSTER_TLS_CA").ok() else {
        return Ok(None);
    };
    use std::io::{Error, ErrorKind};
    let mut roots = RootCertStore::empty();
    let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(&ca)?))
        .collect::<Result<Vec<_>, _>>()?;
    for c in certs {
        roots
            .add(c)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    }
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let cfg = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = std::env::var("ELYRASQL_CLUSTER_TLS_SERVER_NAME")
        .unwrap_or_else(|_| "localhost".to_string());
    let server_name =
        ServerName::try_from(name).map_err(|e| Error::new(ErrorKind::InvalidInput, e))?;
    Ok(Some((TlsConnector::from(Arc::new(cfg)), server_name)))
}

/// A boxable duplex transport (plain TCP or TLS) for the replica connect path
/// and the Raft control plane.
pub(crate) trait DuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> DuplexStream for T {}

/// Process-wide cached client TLS config for outbound cluster connections
/// (Raft RPCs reuse this rather than re-parsing the CA on every call). A load
/// error is cached and returned on every attempted connection so an explicitly
/// configured cluster never silently downgrades to plaintext.
pub(crate) fn cluster_client_tls_cached(
) -> std::io::Result<Option<(TlsConnector, ServerName<'static>)>> {
    use std::sync::OnceLock;
    type CachedTls = Result<Option<(TlsConnector, ServerName<'static>)>, (ErrorKind, String)>;
    static C: OnceLock<CachedTls> = OnceLock::new();
    match C.get_or_init(|| cluster_client_tls().map_err(|e| (e.kind(), e.to_string()))) {
        Ok(tls) => Ok(tls.clone()),
        Err((kind, message)) => Err(Error::new(*kind, message.clone())),
    }
}

/// One framed replication message.
#[derive(Serialize, Deserialize)]
enum ReplMsg {
    /// Replica → primary (first message): the highest LSN the replica has
    /// already applied (0 = fresh replica needing a full snapshot).
    Hello { last_lsn: u64 },
    /// Primary → replica: the requested incremental catch-up is not possible
    /// (binlog disabled or the needed segments were purged); the replica must
    /// wipe its state and reconnect fresh for a full snapshot.
    Resync,
    /// A chunk of the bootstrap snapshot (key/value pairs).
    Snap(Vec<(Vec<u8>, Vec<u8>)>),
    /// End of snapshot / catch-up; state now reflects up to (at least) `lsn`.
    SnapEnd { lsn: u64 },
    /// A committed write-set to apply after the snapshot.
    Write {
        lsn: u64,
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    },
    /// Replica → primary: acknowledge application through `lsn` (semi-sync).
    Ack { lsn: u64 },
}

const SNAP_CHUNK: usize = 4096;
/// Key under which a replica persists its highest applied LSN (so a reconnect
/// can request an incremental catch-up instead of a full re-bootstrap).
const REPL_LSN_KEY: &[u8] = b"meta::repl::lsn";

fn lsn_bytes(lsn: u64) -> (Vec<u8>, Vec<u8>) {
    (REPL_LSN_KEY.to_vec(), lsn.to_le_bytes().to_vec())
}

fn io<E: std::fmt::Display>(e: E) -> Error {
    Error::other(e.to_string())
}

async fn send_msg<W: AsyncWrite + Unpin>(w: &mut W, m: &ReplMsg) -> std::io::Result<()> {
    let bytes = bincode::serialize(m).map_err(io)?;
    w.write_all(&(bytes.len() as u32).to_le_bytes()).await?;
    w.write_all(&bytes).await?;
    Ok(())
}

async fn recv_msg<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Option<ReplMsg>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let n = u32::from_le_bytes(len) as usize;
    if n > elyra_core::max_frame_bytes() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "replication frame too large",
        ));
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(Some(bincode::deserialize(&buf).map_err(io)?))
}

/// Serve the replication endpoint on the primary.
pub async fn serve_replication(addr: String, db: Db) -> std::io::Result<()> {
    // The replication stream hands a full copy of the database to every
    // connecting peer, so it must never be reachable without authentication:
    // any local process (or an SSRF payload) that can open a TCP connection to
    // the port receives every row. Require a cluster secret on every bind,
    // loopback included, unless explicitly overridden with
    // ELYRASQL_ALLOW_OPEN_AUTH=1.
    if !crate::cluster::has_cluster_secret() && !crate::env_flag("ELYRASQL_ALLOW_OPEN_AUTH") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "refusing to start the replication endpoint on {addr} without \
                 authentication: any process that can reach this port receives a \
                 full copy of the database. Set ELYRASQL_CLUSTER_SECRET (shared \
                 by primary and replicas), or set ELYRASQL_ALLOW_OPEN_AUTH=1 to \
                 explicitly accept an unauthenticated replication endpoint."
            ),
        ));
    }
    if !crate::cluster::has_cluster_secret() {
        warn!(%addr, "replication endpoint is UNAUTHENTICATED (ELYRASQL_ALLOW_OPEN_AUTH=1) - any peer that reaches it receives the full data set");
    }
    let tls = cluster_server_tls()?.map(TlsAcceptor::from);
    let listener = crate::listen(&addr, "replication").await?;
    info!(%addr, tls = tls.is_some(), "ElyraSQL replication endpoint listening");
    if tls.is_none() {
        warn!(%addr, "replication transport is UNENCRYPTED - set ELYRASQL_CLUSTER_TLS_CERT/KEY, or run it on a private network/VPN");
    }
    loop {
        let (stream, peer) = listener.accept().await?;
        let db = db.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            info!(%peer, "replica connected");
            let res = match tls {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(s) => handle_replica(s, db).await,
                    Err(e) => Err(e),
                },
                None => handle_replica(stream, db).await,
            };
            if let Err(e) = res {
                warn!(%peer, error = %e, "replica stream ended");
            }
        });
    }
}

async fn handle_replica<S>(mut stream: S, db: Db) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Authenticate the replica (no-op unless a cluster secret is configured).
    // Runs over whatever transport `S` is (plain TCP or a TLS stream).
    crate::cluster::auth_accept(&mut stream).await?;
    // Answer the replica's challenge so it can verify this primary's identity
    // (no-op without a cluster secret). A replica that cannot verify the peer
    // must never start applying its data.
    crate::cluster::auth_respond_to_peer(&mut stream).await?;
    let (mut rd, mut stream) = tokio::io::split(stream);

    // First message tells us how far the replica has already caught up.
    let last_lsn = match recv_msg(&mut rd).await? {
        Some(ReplMsg::Hello { last_lsn }) => last_lsn,
        _ => 0,
    };

    // Register this replica for quorum accounting; deregister on disconnect.
    let replica_id = db.register_replica();

    // Read replica acknowledgements (semi-sync / quorum) on the read half.
    let adb = db.clone();
    tokio::spawn(async move {
        loop {
            match recv_msg(&mut rd).await {
                Ok(Some(ReplMsg::Ack { lsn })) => adb.report_ack(replica_id, lsn),
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
        adb.unregister_replica(replica_id);
    });

    // Subscribe before reading the LSN so no committed write is missed; any
    // write with lsn <= snap_lsn that also appears in the (live) snapshot is
    // applied idempotently on the replica.
    let mut rx = db.repl_subscribe();
    let snap_lsn = db.current_lsn();

    // Incremental catch-up: if the replica already has state and the binlog
    // still covers everything since then, stream only the delta.
    if last_lsn > 0 && last_lsn <= snap_lsn {
        match incremental_catchup(&mut stream, &db, last_lsn, snap_lsn).await? {
            true => {
                info!(
                    last_lsn,
                    snap_lsn, "replica caught up incrementally from binlog"
                );
            }
            false => {
                // Cannot serve incrementally: tell the replica to resync clean.
                send_msg(&mut stream, &ReplMsg::Resync).await?;
                return Ok(());
            }
        }
    } else {
        // Full bootstrap: the whole keyspace, page by page, from one snapshot
        // taken after `snap_lsn` was read, so it holds every write up to it.
        // Each page used to be read in a read transaction of its own, so the
        // copy mixed states the database was in while it was sent.
        let snapshot = db.snapshot().map_err(io)?;
        let mut from: Vec<u8> = Vec::new();
        loop {
            let (snap, start) = (snapshot.clone(), from.clone());
            let batch =
                tokio::task::spawn_blocking(move || snap.scan_range(&start, None, SNAP_CHUNK))
                    .await
                    .map_err(io)?
                    .map_err(io)?;
            if batch.is_empty() {
                break;
            }
            let last = batch.len() < SNAP_CHUNK;
            if let Some((k, _)) = batch.last() {
                from = k.clone();
                from.push(0);
            }
            send_msg(&mut stream, &ReplMsg::Snap(batch)).await?;
            if last {
                break;
            }
        }
        db.report_sent(replica_id, snap_lsn);
        send_msg(&mut stream, &ReplMsg::SnapEnd { lsn: snap_lsn }).await?;
    }

    // Stream subsequent write-sets.
    loop {
        match rx.recv().await {
            Ok(ev) => {
                if ev.lsn > snap_lsn {
                    let WriteEvent {
                        lsn, puts, deletes, ..
                    } = &*ev;
                    db.report_sent(replica_id, *lsn);
                    send_msg(
                        &mut stream,
                        &ReplMsg::Write {
                            lsn: *lsn,
                            puts: puts.clone(),
                            deletes: deletes.clone(),
                        },
                    )
                    .await?;
                }
            }
            Err(RecvError::Lagged(n)) => {
                return Err(io(format!(
                    "replica fell behind by {n} write-sets; reconnect to re-bootstrap"
                )));
            }
            Err(RecvError::Closed) => return Ok(()),
        }
    }
}

/// Stream an incremental catch-up (binlog delta) to a reconnecting replica.
/// Returns `false` if the primary cannot serve it (no binlog, or the needed
/// segments were purged) so the caller can ask the replica to resync.
async fn incremental_catchup<W: AsyncWrite + Unpin>(
    stream: &mut W,
    db: &Db,
    last_lsn: u64,
    snap_lsn: u64,
) -> std::io::Result<bool> {
    let Some(dir) = db.binlog_dir().map(|p| p.to_path_buf()) else {
        return Ok(false);
    };
    // The binlog must still contain the record right after the replica's point.
    let earliest = tokio::task::spawn_blocking({
        let dir = dir.clone();
        move || elyra_storage::binlog::earliest_lsn(&dir)
    })
    .await
    .map_err(io)?
    .map_err(io)?;
    match earliest {
        Some(e) if e <= last_lsn + 1 => {}
        _ => return Ok(false), // gap: segments purged
    }
    let recs = tokio::task::spawn_blocking(move || {
        elyra_storage::binlog::read_since(&dir, last_lsn, snap_lsn)
    })
    .await
    .map_err(io)?
    .map_err(io)?;
    for rec in recs {
        send_msg(
            stream,
            &ReplMsg::Write {
                lsn: rec.lsn,
                puts: rec.puts,
                deletes: rec.deletes,
            },
        )
        .await?;
    }
    send_msg(stream, &ReplMsg::SnapEnd { lsn: snap_lsn }).await?;
    Ok(true)
}

/// Run as a replica: (re)bootstrap from `primary`, then apply the write stream
/// into the local `db`, persisting the applied LSN so a reconnect can catch up
/// incrementally from the primary's binlog. Reconnects on a lost stream,
/// closed or failed; returns when the data cannot be continued (marked for a
/// new bootstrap) or cannot be written locally.
pub async fn run_replica(primary: String, db: Db) -> std::io::Result<()> {
    // Fail closed: a replica applies whatever its "primary" streams to it, so
    // an unauthenticated connection is one spoofed host (or MITM on the
    // default plaintext transport) away from full data corruption. Require a
    // cluster secret unless explicitly overridden.
    if !crate::cluster::has_cluster_secret() && !crate::env_flag("ELYRASQL_ALLOW_OPEN_AUTH") {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "refusing to replicate from {primary} without authentication: \
                 a replica applies everything its primary sends, so the primary \
                 must prove its identity. Set ELYRASQL_CLUSTER_SECRET (the same \
                 value as the primary), or set ELYRASQL_ALLOW_OPEN_AUTH=1 to \
                 explicitly accept an unauthenticated primary."
            ),
        ));
    }
    // Verify-the-primary TLS, if configured (once; reused each reconnect).
    let client_tls = cluster_client_tls()?;
    if client_tls.is_none() {
        warn!(%primary, "replication transport is UNENCRYPTED - set ELYRASQL_CLUSTER_TLS_CA, or run it on a private network/VPN");
    }
    loop {
        let applied = applied_lsn(&db).await;
        let last_lsn = applied.unwrap_or(0);
        let tcp = match TcpStream::connect(&primary).await {
            Ok(s) => s,
            Err(e) => {
                warn!(%primary, error = %e, "connect failed; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
        };
        // Optionally wrap in TLS (the primary's certificate is verified).
        let mut stream: Box<dyn DuplexStream> = match &client_tls {
            Some((connector, name)) => match connector.connect(name.clone(), tcp).await {
                Ok(s) => Box::new(s),
                Err(e) => {
                    warn!(%primary, error = %e, "TLS handshake with primary failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
            },
            None => Box::new(tcp),
        };
        info!(%primary, last_lsn, tls = client_tls.is_some(), "connected to primary");
        // Mutual authentication: the replica proves knowledge of the secret
        // (auth_connect), then requires the primary to prove it too. Without
        // the second step any host accepting connections could impersonate a
        // primary and feed this replica arbitrary data. Bounded by a timeout
        // so a peer that stalls mid-handshake cannot hang the reconnect loop.
        let handshake = async {
            crate::cluster::auth_connect(&mut stream).await?;
            crate::cluster::auth_verify_peer(&mut stream).await
        };
        match tokio::time::timeout(std::time::Duration::from_secs(5), handshake).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(%primary, error = %e, "cluster authentication with primary failed; retrying");
                drop(stream);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            Err(_) => {
                warn!(%primary, "cluster authentication timed out; retrying");
                drop(stream);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
        }
        let (mut rd, mut wr) = tokio::io::split(stream);
        if let Err(e) = send_msg(&mut wr, &ReplMsg::Hello { last_lsn }).await {
            warn!(%primary, error = %e, "lost the primary before streaming; reconnecting");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        }

        let mut snap_pairs = 0u64;
        // A full snapshot is being applied and has not finished: until it has,
        // the data is part old, part new and cannot be continued.
        let mut in_snapshot = false;
        loop {
            let msg = match recv_msg(&mut rd).await {
                Ok(Some(msg)) => msg,
                Ok(None) if in_snapshot => {
                    return Err(rebootstrap(
                        &db,
                        "the primary closed the stream mid-snapshot",
                    ));
                }
                Ok(None) => break,
                Err(e) if in_snapshot => {
                    return Err(rebootstrap(
                        &db,
                        &format!("the snapshot stream failed: {e}"),
                    ));
                }
                // A stream that fails rather than closes -- a primary killed
                // or restarted resets it -- is lost the same way: reconnect
                // and resume. It used to stop replication, and the process
                // exited without the data having been marked for anything.
                Err(e) => {
                    warn!(%primary, error = %e, "the stream from the primary failed");
                    break;
                }
            };
            match msg {
                ReplMsg::Resync => {
                    return Err(rebootstrap(
                        &db,
                        "the primary cannot stream what this replica is missing (its binlog \
                         no longer covers this replica's LSN)",
                    ));
                }
                ReplMsg::Snap(_) if applied.is_some() && !in_snapshot => {
                    return Err(rebootstrap(
                        &db,
                        "the primary sent a full snapshot to a replica that already has data: \
                         its history does not continue this replica's (did it restart \
                         without a binlog?)",
                    ));
                }
                ReplMsg::Snap(pairs) => {
                    if !in_snapshot {
                        db.set_fence(Some(COPYING.into()));
                    }
                    in_snapshot = true;
                    snap_pairs += pairs.len() as u64;
                    db.commit(pairs, Vec::new()).await.map_err(io)?;
                }
                ReplMsg::SnapEnd { lsn } => {
                    db.commit(vec![lsn_bytes(lsn)], Vec::new())
                        .await
                        .map_err(io)?;
                    in_snapshot = false;
                    db.set_fence(None);
                    info!(lsn, rows = snap_pairs, "caught up; streaming writes");
                    if let Err(e) = send_msg(&mut wr, &ReplMsg::Ack { lsn }).await {
                        warn!(%primary, error = %e, "could not acknowledge to the primary");
                        break;
                    }
                }
                ReplMsg::Write {
                    lsn,
                    mut puts,
                    deletes,
                } => {
                    puts.push(lsn_bytes(lsn));
                    db.commit(puts, deletes).await.map_err(io)?;
                    if let Err(e) = send_msg(&mut wr, &ReplMsg::Ack { lsn }).await {
                        warn!(%primary, error = %e, "could not acknowledge to the primary");
                        break;
                    }
                    if lsn % 10_000 == 0 {
                        info!(lsn, "replica caught up");
                    }
                }
                ReplMsg::Hello { .. } | ReplMsg::Ack { .. } => {}
            }
        }
        warn!(%primary, "lost the stream from the primary; reconnecting");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Why a replica refuses queries while it copies the primary's data: until the
/// copy is complete its tables are empty or partly filled.
pub const COPYING: &str = "this replica is copying the primary's data and cannot be queried yet";

/// The file whose presence tells the next start of a replica on `data` to
/// discard its data and bootstrap from the primary again.
pub fn resync_marker(data: &std::path::Path) -> std::path::PathBuf {
    let mut name = data.as_os_str().to_owned();
    name.push(".resync");
    std::path::PathBuf::from(name)
}

/// Stop replicating and have the next start bootstrap again from scratch.
///
/// Used only when this replica's data can no longer be continued: the primary
/// cannot stream what it is missing, or what it would send does not build on
/// it. A full snapshot is applied onto an empty file only: applied over
/// existing data it would add and update keys but never remove one the primary
/// deleted in the meantime.
fn rebootstrap(db: &Db, why: &str) -> Error {
    match db.data_path() {
        Some(data) => {
            if let Err(e) = std::fs::write(resync_marker(&data), why) {
                warn!(error = %e, "could not mark the replica for a new bootstrap");
            }
        }
        None => warn!("in-memory replica cannot be marked for a new bootstrap"),
    }
    io(format!(
        "{why}; the replica bootstraps again on its next start"
    ))
}

/// The replica's persisted highest-applied LSN, or `None` before a bootstrap
/// has completed. It is written with the end of the bootstrap, which can be
/// LSN 0: a primary without a binlog counts writes only while a replica is
/// attached, so 0 does not mean "never bootstrapped".
pub async fn applied_lsn(db: &Db) -> Option<u64> {
    match db.get(REPL_LSN_KEY.to_vec()).await {
        Ok(Some(b)) if b.len() == 8 => Some(u64::from_le_bytes(b.try_into().unwrap())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The guards and handshake helpers read process-global env vars; serialize
    /// the tests that touch them so parallel test threads cannot observe each
    /// other's mutations. Async mutex because the guard is held across `.await`.
    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn set_env(key: &str, value: Option<&str>) {
        match value {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
    }

    /// Run the endpoint's startup guard and cancel it right after a successful
    /// bind (the accept loop runs until cancelled).
    async fn guard(addr: &str) -> std::io::Result<()> {
        let db = Db::in_memory().map_err(|e| Error::other(e.to_string()))?;
        let handle = tokio::spawn(serve_replication(addr.to_string(), db));
        // A bind failure surfaces almost immediately; a success means the
        // listener is up and we can cancel the accept loop.
        for _ in 0..50 {
            if handle.is_finished() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let result = if handle.is_finished() {
            handle.await.expect("guard task")
        } else {
            handle.abort();
            Ok(())
        };
        result.map(|_| ())
    }

    #[tokio::test]
    async fn replication_requires_secret_even_on_loopback() {
        let _env = ENV_LOCK.lock().await;
        set_env("ELYRASQL_CLUSTER_SECRET", None);
        set_env("ELYRASQL_ALLOW_OPEN_AUTH", None);

        // The old behaviour allowed an unauthenticated full-database dump on
        // loopback binds; that must now fail closed.
        let err = guard("127.0.0.1:0").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(err.to_string().contains("ELYRASQL_CLUSTER_SECRET"));
    }

    #[tokio::test]
    async fn replication_starts_with_secret_on_loopback() {
        let _env = ENV_LOCK.lock().await;
        set_env("ELYRASQL_CLUSTER_SECRET", Some("test-secret"));
        set_env("ELYRASQL_ALLOW_OPEN_AUTH", None);
        let res = guard("127.0.0.1:0").await;
        set_env("ELYRASQL_CLUSTER_SECRET", None);
        res.expect("secret-configured endpoint must start");
    }

    #[tokio::test]
    async fn replication_open_auth_requires_explicit_opt_in() {
        let _env = ENV_LOCK.lock().await;
        set_env("ELYRASQL_CLUSTER_SECRET", None);
        set_env("ELYRASQL_ALLOW_OPEN_AUTH", Some("1"));
        let res = guard("127.0.0.1:0").await;
        set_env("ELYRASQL_ALLOW_OPEN_AUTH", None);
        res.expect("explicit opt-in must allow an unauthenticated endpoint");
    }

    #[tokio::test]
    async fn replica_refuses_to_start_without_secret_or_opt_in() {
        let _env = ENV_LOCK.lock().await;
        set_env("ELYRASQL_CLUSTER_SECRET", None);
        set_env("ELYRASQL_ALLOW_OPEN_AUTH", None);
        let db = Db::in_memory().unwrap();
        let err = run_replica("127.0.0.1:1".to_string(), db)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert!(err.to_string().contains("ELYRASQL_CLUSTER_SECRET"));
    }

    /// Both directions of the handshake over one connection, shared secret.
    #[tokio::test]
    async fn mutual_handshake_succeeds_with_shared_secret() {
        let _env = ENV_LOCK.lock().await;
        set_env("ELYRASQL_CLUSTER_SECRET", Some("shared-test-secret"));
        let (mut replica_side, mut primary_side) = tokio::io::duplex(1024);
        let server = tokio::spawn(async move {
            crate::cluster::auth_accept(&mut primary_side).await?;
            crate::cluster::auth_respond_to_peer(&mut primary_side).await
        });
        let client = async {
            crate::cluster::auth_connect(&mut replica_side).await?;
            crate::cluster::auth_verify_peer(&mut replica_side).await
        };
        let (server_res, client_res) = tokio::join!(server, client);
        set_env("ELYRASQL_CLUSTER_SECRET", None);
        server_res.unwrap().expect("primary side handshake");
        client_res.expect("replica side handshake");
    }

    /// A peer that does not know the secret must fail verification. With
    /// mismatched secrets the impersonator actually trips over its own
    /// `auth_accept` (it cannot verify the replica's proof) and closes the
    /// stream; the replica must then observe a failure, never proceed.
    #[tokio::test]
    async fn mutual_handshake_rejects_wrong_secret() {
        let _env = ENV_LOCK.lock().await;
        set_env("ELYRASQL_CLUSTER_SECRET", Some("correct-secret"));
        let (mut replica_side, mut primary_side) = tokio::io::duplex(1024);
        let server = tokio::spawn(async move {
            set_env("ELYRASQL_CLUSTER_SECRET", Some("wrong-secret"));
            let r = async {
                crate::cluster::auth_accept(&mut primary_side).await?;
                crate::cluster::auth_respond_to_peer(&mut primary_side).await
            }
            .await;
            set_env("ELYRASQL_CLUSTER_SECRET", Some("correct-secret"));
            r
        });
        let client = async {
            crate::cluster::auth_connect(&mut replica_side).await?;
            crate::cluster::auth_verify_peer(&mut replica_side).await
        };
        let (server_res, client_res) = tokio::join!(server, client);
        set_env("ELYRASQL_CLUSTER_SECRET", None);
        // The impersonator cannot verify the replica's proof.
        assert!(server_res.unwrap().is_err());
        // The replica must refuse to continue against such a peer.
        assert!(
            client_res.is_err(),
            "replica handshake must fail against a wrong-secret primary"
        );
    }

    /// End-to-end: with a shared secret, a real primary/replica pair makes
    /// progress (snapshot applied); without verification there is no way for
    /// the replica to distinguish this primary from an impersonator.
    #[tokio::test]
    async fn replication_applies_snapshot_with_shared_secret() {
        let _env = ENV_LOCK.lock().await;
        set_env("ELYRASQL_CLUSTER_SECRET", Some("e2e-secret"));
        set_env("ELYRASQL_ALLOW_OPEN_AUTH", None);

        // Reserve a port for the primary's replication endpoint.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let primary_db = Db::in_memory().unwrap();
        primary_db
            .commit(
                vec![(b"test::key".to_vec(), b"test-value".to_vec())],
                Vec::new(),
            )
            .await
            .unwrap();
        let srv_db = primary_db.clone();
        let server = tokio::spawn(serve_replication(format!("127.0.0.1:{port}"), srv_db));

        let replica_db = Db::in_memory().unwrap();
        let repl_task = tokio::spawn(run_replica(format!("127.0.0.1:{port}"), replica_db.clone()));

        // Wait until the replicated key shows up on the replica.
        let mut applied = false;
        for _ in 0..100 {
            if repl_task.is_finished() {
                break;
            }
            if let Ok(Some(v)) = replica_db.get(b"test::key".to_vec()).await {
                if v == b"test-value" {
                    applied = true;
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        repl_task.abort();
        server.abort();
        set_env("ELYRASQL_CLUSTER_SECRET", None);
        assert!(applied, "snapshot key must reach the replica");
    }

    /// A primary killed or restarted resets the stream rather than closing it.
    /// The replica used to stop replicating on that and exit, without its
    /// data marked for anything; it reconnects and resumes now, as it does
    /// when the stream is closed.
    #[tokio::test]
    async fn a_replica_reconnects_when_the_stream_is_reset() {
        let _env = ENV_LOCK.lock().await;
        set_env("ELYRASQL_CLUSTER_SECRET", Some("reset-secret"));
        set_env("ELYRASQL_ALLOW_OPEN_AUTH", None);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let primary = tokio::spawn(async move {
            // First stream: a snapshot, acknowledged, then a reset.
            let (mut s, _) = listener.accept().await.unwrap();
            crate::cluster::auth_accept(&mut s).await.unwrap();
            crate::cluster::auth_respond_to_peer(&mut s).await.unwrap();
            let hello = recv_msg(&mut s).await.unwrap();
            assert!(matches!(hello, Some(ReplMsg::Hello { last_lsn: 0 })));
            let snap = vec![(b"k1".to_vec(), b"v1".to_vec())];
            send_msg(&mut s, &ReplMsg::Snap(snap)).await.unwrap();
            send_msg(&mut s, &ReplMsg::SnapEnd { lsn: 1 })
                .await
                .unwrap();
            let ack = recv_msg(&mut s).await.unwrap();
            assert!(matches!(ack, Some(ReplMsg::Ack { lsn: 1 })));
            #[allow(deprecated)]
            s.set_linger(Some(std::time::Duration::ZERO)).unwrap();
            drop(s);
            // Second stream: the replica is back, from where it was.
            let (mut s, _) = listener.accept().await.unwrap();
            crate::cluster::auth_accept(&mut s).await.unwrap();
            crate::cluster::auth_respond_to_peer(&mut s).await.unwrap();
            let hello = recv_msg(&mut s).await.unwrap();
            let write = ReplMsg::Write {
                lsn: 2,
                puts: vec![(b"k2".to_vec(), b"v2".to_vec())],
                deletes: Vec::new(),
            };
            send_msg(&mut s, &write).await.unwrap();
            let ack = recv_msg(&mut s).await.unwrap();
            (hello, ack)
        });
        let replica_db = Db::in_memory().unwrap();
        let replica = tokio::spawn(run_replica(addr, replica_db.clone()));
        let streams = tokio::time::timeout(std::time::Duration::from_secs(10), primary).await;
        let finished = replica.is_finished();
        replica.abort();
        set_env("ELYRASQL_CLUSTER_SECRET", None);
        let (hello, ack) = streams
            .expect("the replica did not come back after the reset")
            .unwrap();
        assert!(!finished, "the replica stopped replicating");
        assert!(matches!(hello, Some(ReplMsg::Hello { last_lsn: 1 })));
        assert!(matches!(ack, Some(ReplMsg::Ack { lsn: 2 })));
        assert_eq!(
            replica_db.get(b"k2".to_vec()).await.unwrap(),
            Some(b"v2".to_vec())
        );
    }
}
