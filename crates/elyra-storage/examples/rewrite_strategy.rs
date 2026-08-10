//! Compare the current atomic rewrite with a bounded-memory shadow generation.

use std::env;
use std::path::Path;
use std::time::{Duration, Instant};

use elyra_storage::Storage;

type Entry = (Vec<u8>, Vec<u8>);

#[derive(Clone, Copy)]
enum Strategy {
    Atomic,
    Shadow,
}

struct Config {
    strategy: Strategy,
    path: String,
    rows: usize,
    indexes: usize,
    batch_rows: usize,
}

fn parse_config() -> Result<Config, String> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 6 {
        return Err(format!(
            "usage: {} <atomic|shadow> <database-path> <rows> <indexes> <batch-rows>",
            args.first().map_or("rewrite_strategy", String::as_str)
        ));
    }
    let strategy = match args[1].as_str() {
        "atomic" => Strategy::Atomic,
        "shadow" => Strategy::Shadow,
        other => return Err(format!("unknown strategy: {other}")),
    };
    let parse_positive = |value: &str, name: &str| {
        value
            .parse::<usize>()
            .map_err(|error| format!("invalid {name}: {error}"))
            .and_then(|parsed| {
                (parsed > 0)
                    .then_some(parsed)
                    .ok_or_else(|| format!("{name} must be positive"))
            })
    };
    Ok(Config {
        strategy,
        path: args[2].clone(),
        rows: parse_positive(&args[3], "rows")?,
        indexes: args[4]
            .parse()
            .map_err(|error| format!("invalid indexes: {error}"))?,
        batch_rows: parse_positive(&args[5], "batch rows")?,
    })
}

fn data_entry(generation: &str, kind: &str, row: usize) -> Entry {
    let key = format!("{generation}::data::bench::{kind}::{row:016x}").into_bytes();
    let value = format!("payload-{row:016x}-{:048}", row % 10_000).into_bytes();
    (key, value)
}

fn index_entry(generation: &str, kind: &str, index: usize, row: usize) -> Entry {
    let key = format!(
        "{generation}::index::bench::idx{index}::{:08x}::{kind}::{row:016x}",
        row % 1_000
    )
    .into_bytes();
    let value = format!("{generation}::data::bench::{kind}::{row:016x}").into_bytes();
    (key, value)
}

fn row_entries(generation: &str, kind: &str, row: usize, indexes: usize) -> Vec<Entry> {
    let mut entries = Vec::with_capacity(indexes + 1);
    entries.push(data_entry(generation, kind, row));
    entries.extend((0..indexes).map(|index| index_entry(generation, kind, index, row)));
    entries
}

fn seed(storage: &Storage, config: &Config) -> Result<Duration, Box<dyn std::error::Error>> {
    let started = Instant::now();
    for start in (0..config.rows).step_by(config.batch_rows) {
        let end = start.saturating_add(config.batch_rows).min(config.rows);
        let puts = (start..end)
            .flat_map(|row| row_entries("live", "rowid", row, config.indexes))
            .collect::<Vec<_>>();
        storage.apply(&puts, &[])?;
    }
    Ok(started.elapsed())
}

fn atomic_rewrite(
    storage: &Storage,
    config: &Config,
) -> Result<(Duration, Duration, Duration), Box<dyn std::error::Error>> {
    let entry_count = config
        .rows
        .checked_mul(config.indexes.saturating_add(1))
        .ok_or("row and index count overflow")?;
    let mut puts = Vec::with_capacity(entry_count);
    let mut deletes = Vec::with_capacity(entry_count);
    let build_started = Instant::now();
    for row in 0..config.rows {
        puts.extend(row_entries("live", "pk", row, config.indexes));
        deletes.push(data_entry("live", "rowid", row).0);
        deletes.extend((0..config.indexes).map(|index| index_entry("live", "rowid", index, row).0));
    }
    puts.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    deletes.sort_unstable();
    let build = build_started.elapsed();
    let commit_started = Instant::now();
    storage.apply(&puts, &deletes)?;
    Ok((build, commit_started.elapsed(), Duration::ZERO))
}

fn shadow_rewrite(
    storage: &Storage,
    config: &Config,
) -> Result<(Duration, Duration, Duration), Box<dyn std::error::Error>> {
    let build_started = Instant::now();
    for start in (0..config.rows).step_by(config.batch_rows) {
        let end = start.saturating_add(config.batch_rows).min(config.rows);
        let puts = (start..end)
            .flat_map(|row| row_entries("shadow-1", "pk", row, config.indexes))
            .collect::<Vec<_>>();
        storage.apply(&puts, &[])?;
    }
    let build = build_started.elapsed();

    let switch_started = Instant::now();
    storage.apply(
        &[(b"catalog::bench::generation".to_vec(), b"shadow-1".to_vec())],
        &[],
    )?;
    let switch = switch_started.elapsed();

    let cleanup_started = Instant::now();
    for start in (0..config.rows).step_by(config.batch_rows) {
        let end = start.saturating_add(config.batch_rows).min(config.rows);
        let deletes = (start..end)
            .flat_map(|row| {
                let mut keys = Vec::with_capacity(config.indexes + 1);
                keys.push(data_entry("live", "rowid", row).0);
                keys.extend(
                    (0..config.indexes).map(|index| index_entry("live", "rowid", index, row).0),
                );
                keys
            })
            .collect::<Vec<_>>();
        storage.apply(&[], &deletes)?;
    }
    Ok((build, switch, cleanup_started.elapsed()))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_config().map_err(|error| format!("argument error: {error}"))?;
    if Path::new(&config.path).exists() {
        return Err(format!("database path already exists: {}", config.path).into());
    }
    let storage = Storage::open(&config.path)?;
    let seed_time = seed(&storage, &config)?;
    let (build, switch, cleanup) = match config.strategy {
        Strategy::Atomic => atomic_rewrite(&storage, &config)?,
        Strategy::Shadow => shadow_rewrite(&storage, &config)?,
    };
    let expected = config
        .rows
        .checked_mul(config.indexes.saturating_add(1))
        .ok_or("row and index count overflow")? as u64;
    let visible_prefix = match config.strategy {
        Strategy::Atomic => b"live::".as_slice(),
        Strategy::Shadow => b"shadow-1::".as_slice(),
    };
    let visible = storage.count_prefix(visible_prefix)?;
    if visible != expected {
        return Err(format!("expected {expected} visible entries, found {visible}").into());
    }
    if matches!(config.strategy, Strategy::Shadow) {
        let stale = storage.count_prefix(b"live::")?;
        if stale != 0 {
            return Err(format!("cleanup left {stale} stale live entries").into());
        }
        if storage.get(b"catalog::bench::generation")?.as_deref() != Some(b"shadow-1") {
            return Err("generation switch was not persisted".into());
        }
    }

    println!("seed_ms={:.2}", seed_time.as_secs_f64() * 1_000.0);
    println!("build_ms={:.2}", build.as_secs_f64() * 1_000.0);
    println!("switch_ms={:.2}", switch.as_secs_f64() * 1_000.0);
    println!("cleanup_ms={:.2}", cleanup.as_secs_f64() * 1_000.0);
    println!(
        "foreground_ms={:.2}",
        (build + switch).as_secs_f64() * 1_000.0
    );
    Ok(())
}
