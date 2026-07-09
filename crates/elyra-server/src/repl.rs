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

use elyra_storage::{Db, WriteEvent};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::error::RecvError;
use tracing::{info, warn};

/// One framed replication message.
#[derive(Serialize, Deserialize)]
enum ReplMsg {
    /// A chunk of the bootstrap snapshot (key/value pairs).
    Snap(Vec<(Vec<u8>, Vec<u8>)>),
    /// End of snapshot; the snapshot reflects state up to (at least) `lsn`.
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
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(Some(bincode::deserialize(&buf).map_err(io)?))
}

/// Serve the replication endpoint on the primary.
pub async fn serve_replication(addr: String, db: Db) -> std::io::Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    info!(%addr, "ElyraSQL replication endpoint listening");
    loop {
        let (stream, peer) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            info!(%peer, "replica connected");
            if let Err(e) = handle_replica(stream, db).await {
                warn!(%peer, error = %e, "replica stream ended");
            }
        });
    }
}

async fn handle_replica(stream: TcpStream, db: Db) -> std::io::Result<()> {
    let (mut rd, mut stream) = stream.into_split();

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

    // Bootstrap snapshot: page the entire keyspace.
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let batch = db
            .scan_batch(Vec::new(), cursor.clone(), SNAP_CHUNK)
            .await
            .map_err(io)?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < SNAP_CHUNK;
        cursor = batch.last().map(|(k, _)| k.clone());
        send_msg(&mut stream, &ReplMsg::Snap(batch)).await?;
        if last {
            break;
        }
    }
    send_msg(&mut stream, &ReplMsg::SnapEnd { lsn: snap_lsn }).await?;

    // Stream subsequent write-sets.
    loop {
        match rx.recv().await {
            Ok(ev) => {
                if ev.lsn > snap_lsn {
                    let WriteEvent {
                        lsn, puts, deletes, ..
                    } = &*ev;
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

/// Run as a replica: bootstrap from `primary`, then apply the write stream into
/// the local `db`. Returns when the stream ends (the process should restart to
/// re-bootstrap). The caller must start with a fresh (empty) `db`.
pub async fn run_replica(primary: String, db: Db) -> std::io::Result<()> {
    let stream = TcpStream::connect(&primary).await?;
    info!(%primary, "connected to primary; bootstrapping");
    let (mut rd, mut wr) = stream.into_split();
    let mut snap_pairs = 0u64;
    while let Some(msg) = recv_msg(&mut rd).await? {
        match msg {
            ReplMsg::Snap(pairs) => {
                snap_pairs += pairs.len() as u64;
                db.commit(pairs, Vec::new()).await.map_err(io)?;
            }
            ReplMsg::SnapEnd { lsn } => {
                info!(lsn, rows = snap_pairs, "snapshot applied; streaming writes");
                // Acknowledge the snapshot point so semi-sync can proceed.
                send_msg(&mut wr, &ReplMsg::Ack { lsn }).await?;
            }
            ReplMsg::Write { lsn, puts, deletes } => {
                db.commit(puts, deletes).await.map_err(io)?;
                send_msg(&mut wr, &ReplMsg::Ack { lsn }).await?;
                if lsn % 10_000 == 0 {
                    info!(lsn, "replica caught up");
                }
            }
            ReplMsg::Ack { .. } => {}
        }
    }
    Err(io("primary closed the replication stream"))
}
