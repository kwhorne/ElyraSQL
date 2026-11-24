//! Raft clusters of real `elyrasql cluster` processes: replication, failover,
//! disk failures and snapshots, end to end. The harness is in `common`.

mod common;

use common::Cluster;
use mysql_async::prelude::*;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_replicates_writes_and_survives_losing_its_leader() {
    let mut c = Cluster::new("basic", 3);
    c.start_all();
    let leader = c
        .write(
            "CREATE TABLE t (id INT PRIMARY KEY)",
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    for i in 1..=20 {
        c.write(
            &format!("INSERT INTO t VALUES ({i})"),
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|| panic!("insert {i} was not accepted\n{}", c.logs()));
    }
    for id in 1..=3 {
        c.until_count(id, "t", 20).await;
    }
    // A majority remains: a new leader takes the writes and both survivors have
    // everything.
    c.kill(leader);
    for i in 21..=30 {
        c.write(
            &format!("INSERT INTO t VALUES ({i})"),
            Duration::from_secs(20),
        )
        .await
        .unwrap_or_else(|| panic!("no new leader after killing {leader}\n{}", c.logs()));
    }
    for id in c.running() {
        c.until_count(id, "t", 30).await;
    }
    // The old leader comes back as a follower and catches up.
    c.start(leader);
    c.until_count(leader, "t", 30).await;
}

/// Keep only a few entries in each log, so a node that is behind needs a
/// snapshot; send it in small parts, so it takes several.
fn compacting(c: Cluster) -> Cluster {
    c.with_env("ELYRASQL_RAFT_LOG_KEEP_ENTRIES", "3")
        .with_env("ELYRASQL_RAFT_SNAPSHOT_CHUNK_BYTES", "64")
}

async fn insert(c: &Cluster, ids: impl IntoIterator<Item = i64>) {
    for i in ids {
        c.write(
            &format!("INSERT INTO t VALUES ({i})"),
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|| panic!("insert {i} was not accepted\n{}", c.logs()));
    }
}

async fn sum(c: &Cluster, id: u64) -> Option<i64> {
    let mut conn = c.conn(id).await?;
    conn.query_first("SELECT COALESCE(SUM(id), 0) FROM t")
        .await
        .ok()
        .flatten()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_added_after_compaction_catches_up_from_a_snapshot() {
    // A node added later needs entries no node keeps. It used to accept the
    // log's tail and report it matched with an empty database, and once that
    // was refused it could not catch up at all. Now it is sent a snapshot,
    // holds what the others do, and counts toward the quorum.
    let mut c = compacting(Cluster::new("joiner", 3));
    c.start_all();
    c.write(
        "CREATE TABLE t (id INT PRIMARY KEY)",
        Duration::from_secs(30),
    )
    .await
    .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    insert(&c, 1..=20).await;
    for id in 1..=3 {
        c.until_count(id, "t", 20).await;
    }

    let new = c.add_node();
    c.start(new);
    let leader = c.join(new);
    c.until_count(new, "t", 20).await;
    assert!(
        c.log(new).contains("installed a snapshot from the leader"),
        "the new node did not install a snapshot\n{}",
        c.logs()
    );
    // Four members need three: with a follower down, the new node is one.
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    c.kill(follower);
    insert(&c, 21..=25).await;
    c.until_count(new, "t", 25).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_down_past_compaction_catches_up_deletes_included() {
    let mut c = compacting(Cluster::new("behind", 3));
    c.start_all();
    let leader = c
        .write(
            "CREATE TABLE t (id INT PRIMARY KEY)",
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    insert(&c, 1..=20).await;
    for id in 1..=3 {
        c.until_count(id, "t", 20).await;
    }
    let down = (1..=3).find(|&id| id != leader).unwrap();
    c.kill(down);
    // What it misses: rows it holds are deleted, others added, more than the
    // logs keep.
    c.write("DELETE FROM t WHERE id <= 10", Duration::from_secs(10))
        .await
        .unwrap_or_else(|| panic!("the delete was not accepted\n{}", c.logs()));
    insert(&c, 21..=40).await;
    c.start(down);
    c.until_count(down, "t", 30).await;
    assert_eq!(sum(&c, down).await, Some((11..=40).sum::<i64>()));
    assert!(
        c.log(down).contains("installed a snapshot from the leader"),
        "the follower did not install a snapshot\n{}",
        c.logs()
    );
    // And it goes on from there with entries.
    insert(&c, 41..=45).await;
    c.until_count(down, "t", 35).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_that_cannot_write_its_log_acknowledges_nothing() {
    // The Raft log used to be written best effort: a failed write was dropped
    // without a word, and the leader counted itself toward the quorum of
    // entries it did not have. With one follower down, a write was then
    // acknowledged with a single node holding it on disk.
    let mut c = Cluster::new("nodisk", 3);
    c.start_all();
    let leader = c
        .write(
            "CREATE TABLE t (id INT PRIMARY KEY)",
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    for i in 1..=5 {
        c.write(
            &format!("INSERT INTO t VALUES ({i})"),
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|| panic!("insert {i} was not accepted\n{}", c.logs()));
    }
    for id in 1..=3 {
        c.until_count(id, "t", 5).await;
    }
    // The leader's write must land on it: only the leader and one follower
    // are up, and both are needed.
    let down = (1..=3).find(|&id| id != leader).unwrap();
    c.kill(down);
    let leader = c
        .write("INSERT INTO t VALUES (6)", Duration::from_secs(20))
        .await
        .unwrap_or_else(|| panic!("the remaining two nodes took no write\n{}", c.logs()));
    if !c.deny_disk(leader) {
        eprintln!("skipped: file permissions do not stop this user from writing");
        return;
    }
    let accepted = c
        .write("INSERT INTO t VALUES (7)", Duration::from_secs(8))
        .await;
    assert_eq!(
        accepted,
        None,
        "a write was acknowledged while only one of three nodes could store it\n{}",
        c.logs()
    );
    assert!(
        c.log(leader).contains("cannot write the Raft log"),
        "the leader did not report its disk\n{}",
        c.logs()
    );
    // With its disk back and the third node up, the cluster takes writes again
    // and every node ends up with the same rows. The refused write may have been
    // committed since (its outcome was unknown, not failed): 7 rows or 8.
    c.allow_disk(leader);
    c.start(down);
    c.write("INSERT INTO t VALUES (8)", Duration::from_secs(30))
        .await
        .unwrap_or_else(|| panic!("the cluster did not recover\n{}", c.logs()));
    let deadline = Instant::now() + Duration::from_secs(20);
    let rows = loop {
        let mut counts = Vec::new();
        for id in 1..=3 {
            counts.push(c.count(id, "t").await);
        }
        if counts.iter().all(|n| *n == counts[0]) && counts[0].is_some() {
            tokio::time::sleep(Duration::from_millis(600)).await;
            if c.count(leader, "t").await == counts[0] {
                break counts[0].unwrap();
            }
        }
        assert!(
            Instant::now() < deadline,
            "the nodes never agreed: {counts:?}\n{}",
            c.logs()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(rows == 7 || rows == 8, "{rows} rows\n{}", c.logs());
}

#[tokio::test(flavor = "multi_thread")]
async fn every_node_runs_a_recursive_cte() {
    // A recursive CTE wrote its working table as replicated data: on a
    // follower, which may not propose, it failed with "not the leader".
    let mut c = Cluster::new("rcte", 3);
    c.start_all();
    c.write(
        "CREATE TABLE t (id INT PRIMARY KEY)",
        Duration::from_secs(30),
    )
    .await
    .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    for id in 1..=3 {
        c.until_count(id, "t", 0).await;
    }
    for id in 1..=3 {
        let mut conn = c.conn(id).await.expect("connect");
        let n: Option<i64> = conn
            .query_first(
                "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 50) \
                 SELECT SUM(n) FROM seq",
            )
            .await
            .unwrap_or_else(|e| panic!("node {id}: {e}\n{}", c.logs()));
        assert_eq!(n, Some(1275), "node {id}");
    }
}
