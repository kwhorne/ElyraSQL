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
        // Full bootstrap snapshot: page the entire keyspace.
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
    }

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

/// Stream an incremental catch-up (binlog delta) to a reconnecting replica.
/// Returns `false` if the primary cannot serve it (no binlog, or the needed
/// segments were purged) so the caller can ask the replica to resync.
async fn incremental_catchup(
    stream: &mut tokio::net::tcp::OwnedWriteHalf,
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
/// incrementally from the primary's binlog. Reconnects transparently on stream
/// drops; returns only when the primary asks for a clean resync (the caller
/// should wipe the file and restart).
pub async fn run_replica(primary: String, db: Db) -> std::io::Result<()> {
    loop {
        let last_lsn = read_applied_lsn(&db).await;
        let stream = match TcpStream::connect(&primary).await {
            Ok(s) => s,
            Err(e) => {
                warn!(%primary, error = %e, "connect failed; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
        };
        info!(%primary, last_lsn, "connected to primary");
        let (mut rd, mut wr) = stream.into_split();
        send_msg(&mut wr, &ReplMsg::Hello { last_lsn }).await?;

        let mut snap_pairs = 0u64;
        while let Some(msg) = recv_msg(&mut rd).await? {
            match msg {
                ReplMsg::Resync => {
                    return Err(io(
                        "primary requested resync (binlog gap); restart replica to re-bootstrap",
                    ));
                }
                ReplMsg::Snap(pairs) => {
                    snap_pairs += pairs.len() as u64;
                    db.commit(pairs, Vec::new()).await.map_err(io)?;
                }
                ReplMsg::SnapEnd { lsn } => {
                    db.commit(vec![lsn_bytes(lsn)], Vec::new())
                        .await
                        .map_err(io)?;
                    info!(lsn, rows = snap_pairs, "caught up; streaming writes");
                    send_msg(&mut wr, &ReplMsg::Ack { lsn }).await?;
                }
                ReplMsg::Write {
                    lsn,
                    mut puts,
                    deletes,
                } => {
                    puts.push(lsn_bytes(lsn));
                    db.commit(puts, deletes).await.map_err(io)?;
                    send_msg(&mut wr, &ReplMsg::Ack { lsn }).await?;
                    if lsn % 10_000 == 0 {
                        info!(lsn, "replica caught up");
                    }
                }
                ReplMsg::Hello { .. } | ReplMsg::Ack { .. } => {}
            }
        }
        warn!(%primary, "primary closed the stream; reconnecting");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// Read the replica's persisted highest-applied LSN (0 if none).
async fn read_applied_lsn(db: &Db) -> u64 {
    match db.get(REPL_LSN_KEY.to_vec()).await {
        Ok(Some(b)) if b.len() == 8 => u64::from_le_bytes(b.try_into().unwrap()),
        _ => 0,
    }
}
