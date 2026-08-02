#!/usr/bin/env bash

# Profile-Guided Optimization build of the `elyrasql` binary.
#
# Pipeline: instrument -> train -> merge -> rebuild.
# Training workloads: pre-generated SQL dumps (schema+data from testbench fixtures)
#   + Python benchmarks (OLTP, OLAP, late materialisation).
#
# Training writes to --train-data-dir (default target/pgo/train-data). When
# benchmarking the rebuilt binary, use a separate data directory so that
# warm caches from training do not flatter the measurement.
#
# Modes:
#   ./scripts/pgo-build.sh              # full pipeline -> target/dist/elyrasql
#   ./scripts/pgo-build.sh --profile-only  # instrument + train + merge only
#
# Options:
#   --train-data-dir DIR    training data directory (default target/pgo/train-data)
#   --profile-only          stop after merging profiles; print RUSTFLAGS to consume
#
# Env knobs: TRAIN_SQL_DIR (default target/pgo/training-sql),
#            BENCH_PORT (default 3308), BENCH_ROWS (default 50000), OLAP_ROWS (default 500000).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PROFILE_ONLY=0
TRAIN_DATA_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile-only) PROFILE_ONLY=1; shift ;;
    --train-data-dir) TRAIN_DATA_DIR="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
done

PGO_DIR="${PGO_DIR:-$ROOT/target/pgo}"
PROFRAW_DIR="$PGO_DIR/profraw"
PROFDATA="$PGO_DIR/merged.profdata"
TRAIN_SQL_DIR="${TRAIN_SQL_DIR:-$PGO_DIR/training-sql}"
TRAIN_DATA_DIR="${TRAIN_DATA_DIR:-$PGO_DIR/train-data}"
BENCH_PORT="${BENCH_PORT:-3308}"
BENCH_ROWS="${BENCH_ROWS:-50000}"
OLAP_ROWS="${OLAP_ROWS:-500000}"
LATEMAT_ROWS="${LATEMAT_ROWS:-100000}"
BENCH_BIN="$ROOT/target/dist/elyrasql"

# ---- helpers ----------------------------------------------------------------

find_profdata() {
  if command -v llvm-profdata >/dev/null 2>&1; then
    command -v llvm-profdata
    return
  fi
  local sysroot host cand
  sysroot="$(rustc --print sysroot)"
  host="$(rustc -vV | sed -n 's/host: //p')"
  cand="$sysroot/lib/rustlib/$host/bin/llvm-profdata"
  if [[ -x "$cand" ]]; then
    echo "$cand"
    return
  fi
  echo "ERROR: llvm-profdata not found. Run: rustup component add llvm-tools-preview" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "ERROR: $1 not found" >&2; exit 1; }
}

# ---- preamble ---------------------------------------------------------------

PROFDATA_BIN="$(find_profdata)"
require_cmd python3
require_cmd mysql

if [[ ! -d "$TRAIN_SQL_DIR" ]] || [[ -z "$(ls -A "$TRAIN_SQL_DIR"/*.sql 2>/dev/null)" ]]; then
  echo "ERROR: no training SQL dumps found in $TRAIN_SQL_DIR" >&2
  echo "Generate them with: just stress-data <model> <rows> <batch>" >&2
  exit 1
fi

# ---- stage 1: instrumented build --------------------------------------------

rm -rf "$PROFRAW_DIR" "$PROFDATA"
mkdir -p "$PROFRAW_DIR" "$TRAIN_DATA_DIR"

echo "==> [1/4] instrumented build"
RUSTFLAGS="-Cprofile-generate=$PROFRAW_DIR" cargo build --profile dist --locked -p elyra-cli

# ---- stage 2: training ------------------------------------------------------

echo "==> [2/4] training"

start_server() {
  local data_file="$1"
  LLVM_PROFILE_FILE="$PROFRAW_DIR/elyra-%p-%m.profraw" \
  "$BENCH_BIN" serve \
    --data "$data_file" \
    --listen "127.0.0.1:$BENCH_PORT" \
    --password "" \
    &>/tmp/elyra-pgo-server.log &
  ELYSQL_PID=$!
  for i in $(seq 1 30); do
    if mysql -h 127.0.0.1 -P "$BENCH_PORT" -u root -e "SELECT 1" &>/dev/null; then
      echo "    server ready after ${i}s"
      return 0
    fi
    sleep 1
  done
  echo "    ERROR: server did not start" >&2
  return 1
}

stop_server() {
  kill -INT "$ELYSQL_PID" 2>/dev/null || true
  wait "$ELYSQL_PID" 2>/dev/null || true
}

# Feed each pre-generated SQL dump against a fresh server (models share table names).
i=0
for f in "$TRAIN_SQL_DIR"/*.sql; do
  name="$(basename "$f" .sql)"
  db="$TRAIN_DATA_DIR/train-$i-$name.edb"
  echo "    [$i] ingesting $name ..."
  start_server "$db"
  mysql -h 127.0.0.1 -P "$BENCH_PORT" -u root < "$f" || true
  stop_server
  i=$((i + 1))
done

# Python benchmarks (OLTP, OLAP, late materialisation) on one server.
echo "    running Python benchmarks ..."
db="$TRAIN_DATA_DIR/train-bench.edb"
start_server "$db"
python3 bench/benchmark.py --port "$BENCH_PORT" --rows "$BENCH_ROWS" --password "" || true
python3 bench/olap.py --rows "$OLAP_ROWS" --engines elyra --elyra-port "$BENCH_PORT" --elyra-password "" || true
python3 bench/latemat.py --port "$BENCH_PORT" --rows "$LATEMAT_ROWS" --password "" --label "ElyraSQL-pgo-train" || true
stop_server

profraw_count=$(ls "$PROFRAW_DIR"/*.profraw 2>/dev/null | wc -l | tr -d ' ')
echo "    collected $profraw_count .profraw file(s)"

if [[ "$profraw_count" -eq 0 ]]; then
  echo "ERROR: no .profraw files produced — training run may have failed" >&2
  exit 1
fi

# ---- stage 3: merge profiles ------------------------------------------------

echo "==> [3/4] merge profiles"
"$PROFDATA_BIN" merge -o "$PROFDATA" "$PROFRAW_DIR"
echo "    profile: $PROFDATA ($(du -h "$PROFDATA" | cut -f1))"

# ---- stage 4: optimized rebuild (skip if --profile-only) --------------------

if [[ "$PROFILE_ONLY" == "1" ]]; then
  echo "==> profile-only: skipping optimized rebuild. Consume with:"
  echo "    RUSTFLAGS=\"-Cprofile-use=$PROFDATA -Cllvm-args=-pgo-warn-missing-function\" cargo build --profile dist --locked -p elyra-cli"
  exit 0
fi

echo "==> [4/4] optimized build (profile-use)"
RUSTFLAGS="-Cprofile-use=$PROFDATA -Cllvm-args=-pgo-warn-missing-function" \
  cargo build --profile dist --locked -p elyra-cli
echo "PGO build complete: $BENCH_BIN"
