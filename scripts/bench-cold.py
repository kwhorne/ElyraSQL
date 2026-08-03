#!/usr/bin/env python3
"""Cold-state benchmark runner: compares two elyrasql binaries.

For each workload, starts a FRESH server with a FRESH data directory,
runs the benchmark, kills the server, and deletes the data dir.
This ensures no warm-cache bias between runs.

Usage:
  python3 scripts/bench-cold.py --bin-a path/to/elyrasql-1 --bin-b path/to/elyrasql-2
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time
import signal
import json


def start_server(binary, data_file, port, log_file):
    """Start elyrasql server and return the process."""
    env = os.environ.copy()
    proc = subprocess.Popen(
        [binary, "serve", "--data", data_file, "--listen", f"127.0.0.1:{port}", "--password", ""],
        stdout=open(log_file, "w"),
        stderr=subprocess.STDOUT,
        env=env,
    )
    for _ in range(30):
        try:
            result = subprocess.run(
                ["mysql", "-h", "127.0.0.1", "-P", str(port), "-u", "root", "-e", "SELECT 1"],
                capture_output=True,
                timeout=5,
            )
            if result.returncode == 0:
                return proc
        except (subprocess.TimeoutExpired, Exception):
            pass
        time.sleep(1)
    proc.kill()
    raise RuntimeError(f"Server did not start within 30s")


def stop_server(proc):
    """Send SIGINT and wait for clean exit."""
    try:
        proc.send_signal(signal.SIGINT)
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def run_benchmarks(binary, label, port, rows):
    """Run all 3 benchmarks against a fresh server and return timing report."""
    results = []

    def bench_one(name, cmd, data_dir):
        db_file = os.path.join(data_dir, f"bench.edb")
        log_file = os.path.join(data_dir, "server.log")

        print(f"  [{label}] {name} ...", end=" ", flush=True)
        server = start_server(binary, db_file, port, log_file)

        t0 = time.time()
        try:
            subprocess.run(cmd, check=False, timeout=300,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            pass
        elapsed = time.time() - t0
        print(f"{elapsed*1000:.0f} ms")
        results.append({"workload": name, "binary": label, "elapsed_ms": round(elapsed * 1000)})
        stop_server(server)

    # OLTP: benchmark.py
    oltp_data = tempfile.mkdtemp(prefix="bench-oltp-")
    bench_one("oltp:bulk_insert", ["python3", "bench/benchmark.py", "--port", str(port),
                 "--rows", str(rows), "--password", ""], oltp_data)

    # OLAP: olap.py
    olap_data = tempfile.mkdtemp(prefix="bench-olap-")
    bench_one("olap:aggregation", ["python3", "bench/olap.py", "--rows", str(rows * 10),
                 "--engines", "elyra", "--elyra-port", str(port), "--elyra-password", ""], olap_data)

    # Late materialisation
    latemat_data = tempfile.mkdtemp(prefix="bench-latemat-")
    bench_one("latemat:order_limit", ["python3", "bench/latemat.py", "--port", str(port),
                 "--rows", str(rows), "--password", "", "--label", f"cold-{label}"], latemat_data)

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-a", required=True, help="First binary to benchmark")
    ap.add_argument("--bin-b", required=True, help="Second binary to benchmark")
    ap.add_argument("--label-a", default="baseline", help="Label for first binary")
    ap.add_argument("--label-b", default="pgo", help="Label for second binary")
    ap.add_argument("--port-a", type=int, default=3307, help="Port for binary A")
    ap.add_argument("--port-b", type=int, default=3308, help="Port for binary B")
    ap.add_argument("--rows", type=int, default=10000, help="Row count for benchmarks")
    ap.add_argument("--json", help="Output JSON file")
    args = ap.parse_args()

    print(f"=== Cold benchmarks: {args.label_a} vs {args.label_b} ===")
    print(f"Rows per benchmark: {args.rows}")
    print()

    all_results = []

    print(f"--- {args.label_a} ---")
    results_a = run_benchmarks(args.bin_a, args.label_a, args.port_a, args.rows)
    all_results.extend(results_a)
    print()

    print(f"--- {args.label_b} ---")
    results_b = run_benchmarks(args.bin_b, args.label_b, args.port_b, args.rows)
    all_results.extend(results_b)
    print()

    # Comparison
    print("=== Summary ===")
    by_workload = {}
    for r in all_results:
        key = r["workload"]
        by_workload.setdefault(key, {})
        by_workload[key][r["binary"]] = r["elapsed_ms"]

    total_a = total_b = 0
    for workload, times in by_workload.items():
        a = times.get(args.label_a, 0)
        b = times.get(args.label_b, 0)
        total_a += a
        total_b += b
        if a > 0:
            delta = (b - a) / a * 100
            sign = "+" if delta > 0 else ""
            print(f"  {workload:30s}  {a:>8,d} ms → {b:>8,d} ms  ({sign}{delta:+.1f}%)")
        else:
            print(f"  {workload:30s}  {a:>8,d} ms → {b:>8,d} ms")

    if total_a > 0:
        delta = (total_b - total_a) / total_a * 100
        print(f"  {'TOTAL':30s}  {total_a:>8,d} ms → {total_b:>8,d} ms  ({delta:+.1f}%)")

    if args.json:
        with open(args.json, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\nResults written to {args.json}")


if __name__ == "__main__":
    main()
