#!/usr/bin/env python3
"""Per-workload cold benchmark: baseline vs PGO binary.

Protocol (matches the review request on PR #63): for every single run,
start a FRESH server on a FRESH data directory, run bench/benchmark.py
against it, parse the per-workload timings from its table, kill the
server, then delete the data directory — including the <file>.edb.vidx
sibling directory that holds vector indexes (same trap as issue #58),
so no warm cache or persisted index can flatter a later run.

Runs are interleaved between the two binaries (alternating order) to
cancel out machine-state drift.

Usage:
  python3 scripts/bench-cold-workloads.py \
      --bin-a /tmp/elyrasql-dist-baseline --bin-b /tmp/elyrasql-pgo \
      [--rows 100000] [--runs 3] [--port 3310] [--json out.json]
"""

import argparse
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time

# Rows from the benchmark.py table label, e.g. "full scan COUNT (no index)".
WORKLOADS = [
    ("full scan COUNT", "full scan COUNT"),
    ("GROUP BY age", "GROUP BY age"),
    ("selective join (index NLJ)", "selective join (index NLJ)"),
    ("vector ANN build", "vector ANN build+query"),
]


def start_server(binary, data_file, port, log_file):
    proc = subprocess.Popen(
        [binary, "serve", "--data", data_file, "--listen", f"127.0.0.1:{port}", "--password", ""],
        stdout=open(log_file, "w"),
        stderr=subprocess.STDOUT,
    )
    for _ in range(30):
        try:
            r = subprocess.run(
                ["mysql", "-h", "127.0.0.1", "-P", str(port), "-u", "root", "-e", "SELECT 1"],
                capture_output=True, timeout=5,
            )
            if r.returncode == 0:
                return proc
        except Exception:
            pass
        time.sleep(1)
    proc.kill()
    raise RuntimeError("server did not start within 30s")


def stop_server(proc):
    try:
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def parse_workloads(output):
    """Extract {workload: median_ms} from a benchmark.py report table."""
    found = {}
    for line in output.splitlines():
        for name, prefix in WORKLOADS:
            if line.startswith(prefix):
                m = re.search(r"([\d,]+(?:\.\d+)?)\s*(ms|s)\b", line)
                if m:
                    val = float(m.group(1).replace(",", ""))
                    found[name] = val * 1000.0 if m.group(2) == "s" else val
    return found


def cold_run(binary, port, rows, workdir):
    """One run: fresh data dir + fresh server + benchmark.py, then wipe."""
    data_dir = tempfile.mkdtemp(prefix="cold-", dir=workdir)
    db_file = os.path.join(data_dir, "bench.edb")
    log_file = os.path.join(data_dir, "server.log")
    server = start_server(binary, db_file, port, log_file)
    try:
        r = subprocess.run(
            ["python3", "bench/benchmark.py", "--port", str(port), "--rows", str(rows), "--password", ""],
            capture_output=True, text=True, timeout=1800,
        )
        if r.returncode != 0:
            sys.stderr.write(r.stdout[-2000:] + "\n" + r.stderr[-2000:] + "\n")
            raise RuntimeError("benchmark.py failed")
        return parse_workloads(r.stdout)
    finally:
        stop_server(server)
        # rm -rf the whole dir: removes bench.edb AND the bench.edb.vidx
        # sibling directory (vector index), so the next run is truly cold.
        shutil.rmtree(data_dir, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-a", required=True)
    ap.add_argument("--bin-b", required=True)
    ap.add_argument("--label-a", default="baseline")
    ap.add_argument("--label-b", default="pgo")
    ap.add_argument("--port", type=int, default=3310)
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--json", help="write raw results to this JSON file")
    a = ap.parse_args()

    binaries = [(a.label_a, a.bin_a), (a.label_b, a.bin_b)]
    results = {name: {label: [] for label, _ in binaries} for name, _ in WORKLOADS}
    workdir = tempfile.mkdtemp(prefix="cold-bench-")

    print(f"=== Cold per-workload benchmark: {a.label_a} vs {a.label_b} ===")
    print(f"rows={a.rows:,}  runs={a.runs}  port={a.port}")
    print("Each run: fresh data dir + fresh server; data deleted afterwards.\n")

    for run in range(a.runs):
        order = binaries if run % 2 == 0 else list(reversed(binaries))
        for label, binary in order:
            print(f"[run {run + 1}/{a.runs}] {label} ...", flush=True)
            t0 = time.time()
            got = cold_run(binary, a.port, a.rows, workdir)
            for name, _ in WORKLOADS:
                if name in got:
                    results[name][label].append(got[name])
            missing = [n for n, _ in WORKLOADS if n not in got]
            if missing:
                print(f"  WARNING: could not parse: {missing}", file=sys.stderr)
            print(f"  done in {time.time() - t0:.0f}s: " +
                  ", ".join(f"{n}={got[n]:,.0f}ms" for n, _ in WORKLOADS if n in got), flush=True)

    shutil.rmtree(workdir, ignore_errors=True)

    print(f"\n{'workload':<28}", end="")
    for label, _ in binaries:
        print(f"{label + ' runs (ms)':>34}", end="")
    print(f"{'median ' + a.label_a:>16}{'median ' + a.label_b:>16}{'delta':>9}")
    print("-" * 137)
    summary = []
    for name, _ in WORKLOADS:
        ra, rb = results[name][a.label_a], results[name][a.label_b]
        if not ra or not rb:
            print(f"{name:<28}  MISSING DATA")
            continue
        ma, mb = statistics.median(ra), statistics.median(rb)
        delta = (mb - ma) / ma * 100
        fmt = lambda xs: " ".join(f"{x:>10,.0f}" for x in xs)
        print(f"{name:<28}{fmt(ra):>34}{fmt(rb):>34}{ma:>16,.0f}{mb:>16,.0f}{delta:>+8.1f}%")
        summary.append({"workload": name, "runs": {a.label_a: ra, a.label_b: rb},
                        "median_ms": {a.label_a: ma, a.label_b: mb}, "delta_pct": round(delta, 2)})

    if a.json:
        with open(a.json, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"\nRaw results written to {a.json}")


if __name__ == "__main__":
    main()
