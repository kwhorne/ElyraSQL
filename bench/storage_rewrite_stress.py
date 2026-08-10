#!/usr/bin/env python3
"""Crash/recovery and sustained-load stress test for shadow table rewrites."""

import argparse
import collections
import os
import random
import signal
import statistics
import subprocess
import threading
import time

import pymysql


def is_connection_error(error):
    return isinstance(error, pymysql.InterfaceError) or (
        bool(error.args) and error.args[0] in {0, 2002, 2003, 2006, 2013, 2055}
    )


class Campaign:
    def __init__(self, args):
        self.args = args
        self.deadline = time.monotonic() + args.duration
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.server_lock = threading.Lock()
        self.server = None
        self.server_generation = 0
        self.metrics = collections.Counter()
        self.latencies = collections.defaultdict(list)
        self.errors = []
        self.samples = []
        self.started_at = None
        self.crash_lock = threading.Lock()

    def record(self, name, started):
        elapsed_ms = (time.perf_counter() - started) * 1000
        with self.lock:
            self.metrics[name] += 1
            self.latencies[name].append(elapsed_ms)

    def expected_error(self, name):
        with self.lock:
            self.metrics[name] += 1

    def fail(self, where, error):
        with self.lock:
            self.errors.append(f"{where}: {error}")
        self.stop.set()

    def connect(self):
        return pymysql.connect(
            host="127.0.0.1",
            port=self.args.port,
            user="root",
            autocommit=True,
            connect_timeout=1,
            read_timeout=30,
            write_timeout=30,
        )

    def spawn_server(self):
        with self.server_lock:
            log = open(self.args.log, "ab", buffering=0)
            self.server = subprocess.Popen(
                [
                    self.args.server,
                    "serve",
                    "--data",
                    self.args.data,
                    "--listen",
                    f"127.0.0.1:{self.args.port}",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            self.server_generation += 1

    def start_server(self):
        last_error = None
        for attempt in range(self.args.restart_attempts):
            self.spawn_server()
            for _ in range(100):
                if self.server.poll() is not None:
                    last_error = RuntimeError(f"server exited with {self.server.returncode}")
                    self.server.wait(timeout=10)
                    self.expected_error("startup_open_failures")
                    break
                try:
                    connection = self.connect()
                    connection.close()
                    return
                except pymysql.MySQLError:
                    time.sleep(0.05)
            else:
                last_error = RuntimeError("server did not accept connections")
                with self.server_lock:
                    process = self.server
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=10)
                self.expected_error("startup_open_failures")
            if attempt + 1 < self.args.restart_attempts:
                time.sleep(0.05)
        raise last_error

    def crash_and_restart(self):
        with self.crash_lock:
            with self.server_lock:
                process = self.server
            if process and process.poll() is None:
                if random.random() < self.args.stop_before_kill_probability:
                    os.kill(process.pid, signal.SIGSTOP)
                    time.sleep(random.uniform(0.001, 0.05))
                    self.expected_error("stop_then_kill_crashes")
                os.kill(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
                self.expected_error("forced_crashes")
            time.sleep(random.uniform(0.001, 0.05))

            startup_crashes = 0
            while (
                startup_crashes < self.args.max_startup_crashes
                and random.random() < self.args.startup_crash_probability
            ):
                self.spawn_server()
                time.sleep(random.uniform(0.001, 0.05))
                with self.server_lock:
                    startup_process = self.server
                if startup_process.poll() is None:
                    os.kill(startup_process.pid, signal.SIGKILL)
                startup_process.wait(timeout=10)
                startup_crashes += 1
                self.expected_error("startup_recovery_crashes")

            self.start_server()
            self.verify_durable_invariants("post-crash")
            self.expected_error("successful_restarts")

    def stop_server(self):
        with self.server_lock:
            process = self.server
        if not process or process.poll() is not None:
            return
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    def setup(self):
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS stress_accounts")
        cursor.execute("DROP TABLE IF EXISTS stress_sentinel")
        cursor.execute("DROP TABLE IF EXISTS stress_atomic")
        cursor.execute("DROP TABLE IF EXISTS stress_atomic_meta")
        cursor.execute(
            "CREATE TABLE stress_accounts "
            "(id BIGINT PRIMARY KEY, balance BIGINT, INDEX balance_idx(balance))"
        )
        values = ",".join(f"({row},1000)" for row in range(1000))
        cursor.execute(f"INSERT INTO stress_accounts VALUES {values}")
        cursor.execute(
            "CREATE TABLE stress_sentinel "
            "(id BIGINT PRIMARY KEY, grp BIGINT, checksum BIGINT, payload VARCHAR(64), "
            " INDEX grp_idx(grp), UNIQUE checksum_idx(checksum))"
        )
        for start in range(0, 20_000, 1000):
            values = ",".join(
                f"({row},{row % 97},{row * 17 + 3},'sentinel-{row}')"
                for row in range(start, start + 1000)
            )
            cursor.execute(f"INSERT INTO stress_sentinel VALUES {values}")
        cursor.execute(
            "CREATE TABLE stress_atomic (epoch BIGINT PRIMARY KEY, checksum BIGINT)"
        )
        cursor.execute(
            "CREATE TABLE stress_atomic_meta (id BIGINT PRIMARY KEY, committed_epoch BIGINT)"
        )
        cursor.execute("INSERT INTO stress_atomic_meta VALUES (1,0)")
        cursor.close()
        connection.close()
        self.verify_durable_invariants("setup")

    def verify_durable_invariants(self, where):
        connection = self.connect()
        cursor = connection.cursor()
        connection.begin()
        cursor.execute("SELECT COUNT(*), SUM(balance) FROM stress_accounts")
        count, balance = cursor.fetchone()
        if (count, balance) != (1000, 1_000_000):
            raise AssertionError(f"account invariant {(count, balance)}")
        cursor.execute(
            "SELECT COUNT(*), SUM(id), SUM(checksum), MIN(id), MAX(id) "
            "FROM stress_sentinel"
        )
        expected_sum = 19_999 * 20_000 // 2
        expected_checksum = expected_sum * 17 + 3 * 20_000
        actual = cursor.fetchone()
        expected = (20_000, expected_sum, expected_checksum, 0, 19_999)
        if actual != expected:
            raise AssertionError(f"sentinel invariant {actual} != {expected}")
        group = random.randrange(97)
        cursor.execute("SELECT COUNT(*) FROM stress_sentinel WHERE grp=%s", (group,))
        indexed = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*) FROM stress_sentinel WHERE grp + 0=%s", (group,)
        )
        scanned = cursor.fetchone()[0]
        if indexed != scanned:
            raise AssertionError(f"index mismatch for grp {group}: {indexed} != {scanned}")
        cursor.execute("SELECT committed_epoch FROM stress_atomic_meta WHERE id=1")
        epoch = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*), COALESCE(SUM(checksum),0), COALESCE(MAX(epoch),0) "
            "FROM stress_atomic"
        )
        atomic = tuple(map(int, cursor.fetchone()))
        expected_atomic = (epoch, epoch * (epoch + 1) // 2 * 31 + epoch * 7, epoch)
        if atomic != expected_atomic:
            raise AssertionError(f"atomicity oracle {atomic} != {expected_atomic}")
        connection.commit()
        cursor.close()
        connection.close()
        self.expected_error(f"invariant_checks_{where}")

    def reconnecting_worker(self, name, operation):
        connection = None
        while not self.stop.is_set() and time.monotonic() < self.deadline:
            try:
                if connection is None:
                    connection = self.connect()
                operation(connection)
            except (pymysql.OperationalError, pymysql.InterfaceError) as error:
                if not is_connection_error(error):
                    self.fail(name, error)
                    break
                self.expected_error(f"{name}_reconnects")
                if connection is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass
                connection = None
                time.sleep(0.02)
            except Exception as error:
                self.fail(name, error)
        if connection is not None:
            connection.close()

    def transfer(self, connection):
        left = random.randrange(1000)
        right = (left + random.randrange(1, 1000)) % 1000
        started = time.perf_counter()
        cursor = connection.cursor()
        try:
            connection.begin()
            cursor.execute(
                "UPDATE stress_accounts SET balance=balance-1 WHERE id=%s", (left,)
            )
            cursor.execute(
                "UPDATE stress_accounts SET balance=balance+1 WHERE id=%s", (right,)
            )
            connection.commit()
            self.record("transfers", started)
        except pymysql.MySQLError as error:
            if is_connection_error(error):
                raise
            connection.rollback()
            if error.args and error.args[0] == 1213:
                self.expected_error("transfer_conflicts")
            else:
                raise
        finally:
            cursor.close()

    def indexed_read(self, connection):
        group = random.randrange(97)
        started = time.perf_counter()
        cursor = connection.cursor()
        cursor.execute(
            "SELECT COUNT(*), SUM(checksum) FROM stress_sentinel WHERE grp=%s", (group,)
        )
        indexed = cursor.fetchone()
        if random.randrange(100) == 0:
            cursor.execute(
                "SELECT COUNT(*), SUM(checksum) FROM stress_sentinel WHERE grp + 0=%s",
                (group,),
            )
            if indexed != cursor.fetchone():
                raise AssertionError(f"indexed/scanned result mismatch for {group}")
            self.expected_error("online_index_cross_checks")
        cursor.close()
        self.record("indexed_reads", started)

    def atomic_epoch(self, connection):
        started = time.perf_counter()
        cursor = connection.cursor()
        try:
            connection.begin()
            cursor.execute("SELECT committed_epoch FROM stress_atomic_meta WHERE id=1")
            epoch = cursor.fetchone()[0] + 1
            cursor.execute(
                "INSERT INTO stress_atomic VALUES (%s,%s)", (epoch, epoch * 31 + 7)
            )
            cursor.execute(
                "UPDATE stress_atomic_meta SET committed_epoch=%s WHERE id=1", (epoch,)
            )
            connection.commit()
            self.record("atomic_epochs", started)
        except pymysql.MySQLError as error:
            if is_connection_error(error):
                raise
            connection.rollback()
            if error.args and error.args[0] == 1213:
                self.expected_error("atomic_epoch_conflicts")
            else:
                raise
        finally:
            cursor.close()

    def rewrite_loop(self):
        cycle = 0
        while not self.stop.is_set() and time.monotonic() < self.deadline:
            table = f"stress_rewrite_{cycle % 4}"
            cycle += 1
            connection = None
            try:
                connection = self.connect()
                cursor = connection.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                cursor.execute(
                    f"CREATE TABLE {table} "
                    "(id BIGINT, grp BIGINT, payload VARCHAR(64), INDEX grp_idx(grp))"
                )
                rows = random.choice((20_000, 50_000, 100_000))
                expected_sum = rows * (rows - 1) // 2
                for start in range(0, rows, 1000):
                    values = ",".join(
                        f"({row},{row % 251},'rewrite-{cycle}-{row}')"
                        for row in range(start, min(rows, start + 1000))
                    )
                    cursor.execute(f"INSERT INTO {table} VALUES {values}")
                started = time.perf_counter()
                cursor.execute(f"ALTER TABLE {table} ADD PRIMARY KEY (id)")
                self.record("shadow_rewrites", started)
                cursor.execute(f"SELECT COUNT(*), SUM(id), MIN(id), MAX(id) FROM {table}")
                actual = cursor.fetchone()
                expected = (rows, expected_sum, 0, rows - 1)
                if actual != expected:
                    raise AssertionError(f"{table} aggregate {actual} != {expected}")
                probe = random.randrange(251)
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE grp=%s", (probe,))
                indexed = cursor.fetchone()[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE grp + 0=%s", (probe,))
                if indexed != cursor.fetchone()[0]:
                    raise AssertionError(f"{table} secondary index mismatch")
                cursor.execute(f"UPDATE {table} SET payload='changed' WHERE id=%s", (rows // 2,))
                cursor.execute(f"DELETE FROM {table} WHERE id=%s", (rows // 3,))
                cursor.execute(f"INSERT INTO {table} VALUES (%s,7,'replacement')", (rows + 1,))
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                if cursor.fetchone()[0] != rows:
                    raise AssertionError(f"{table} post-rewrite DML count changed")
                cursor.execute(f"DROP TABLE {table}")
                cursor.close()
            except (pymysql.OperationalError, pymysql.InterfaceError) as error:
                if is_connection_error(error):
                    self.expected_error("rewrite_crash_interrupts")
                else:
                    self.fail("rewrite", error)
            except Exception as error:
                self.fail("rewrite", error)
            finally:
                if connection is not None:
                    try:
                        connection.close()
                    except Exception:
                        pass

    def invalid_rewrite_loop(self):
        cycle = 0
        while not self.stop.is_set() and time.monotonic() < self.deadline:
            table = f"stress_invalid_{cycle % 2}"
            cycle += 1
            try:
                connection = self.connect()
                cursor = connection.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                cursor.execute(f"CREATE TABLE {table} (id BIGINT, payload VARCHAR(32))")
                cursor.execute(
                    f"INSERT INTO {table} VALUES (1,'a'),(1,'duplicate'),(NULL,'null')"
                )
                try:
                    cursor.execute(f"ALTER TABLE {table} ADD PRIMARY KEY (id)")
                    raise AssertionError("invalid primary key rewrite succeeded")
                except pymysql.MySQLError:
                    self.expected_error("rejected_invalid_rewrites")
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                if cursor.fetchone()[0] != 3:
                    raise AssertionError("failed rewrite lost source rows")
                cursor.execute(f"INSERT INTO {table} VALUES (2,'still-rowid')")
                cursor.execute(f"DROP TABLE {table}")
                cursor.close()
                connection.close()
            except (pymysql.OperationalError, pymysql.InterfaceError) as error:
                if is_connection_error(error):
                    self.expected_error("invalid_rewrite_crash_interrupts")
                else:
                    self.fail("invalid-rewrite", error)
            except Exception as error:
                self.fail("invalid-rewrite", error)

    def monitor(self):
        while not self.stop.is_set() and time.monotonic() < self.deadline:
            with self.server_lock:
                process = self.server
                generation = self.server_generation
            if process and process.poll() is None:
                try:
                    output = subprocess.check_output(
                        [
                            "ps",
                            "-o",
                            "rss=,%cpu=,inblock=,oublock=",
                            "-p",
                            str(process.pid),
                        ],
                        text=True,
                    ).split()
                    rss_mib = int(output[0]) / 1024
                    cpu = float(output[1])
                    reads = int(output[2]) if output[2] != "-" else -1
                    writes = int(output[3]) if output[3] != "-" else -1
                    size_mib = os.path.getsize(self.args.data) / (1024 * 1024)
                    if rss_mib > 0:
                        with self.lock:
                            self.samples.append(
                                (
                                    time.monotonic(),
                                    generation,
                                    rss_mib,
                                    cpu,
                                    reads,
                                    writes,
                                    size_mib,
                                )
                            )
                except (OSError, subprocess.SubprocessError, ValueError):
                    pass
            time.sleep(1)

    def random_crash_loop(self):
        while not self.stop.is_set() and time.monotonic() < self.deadline:
            if random.random() < self.args.crash_long_probability:
                delay_ms = random.uniform(self.args.crash_max_ms, self.args.crash_long_max_ms)
            else:
                delay_ms = random.uniform(self.args.crash_min_ms, self.args.crash_max_ms)
            delay = delay_ms / 1000
            if self.stop.wait(delay):
                return
            try:
                self.crash_and_restart()
            except Exception as error:
                self.fail("random-crash-recovery", error)
                return

    def run(self):
        self.started_at = time.monotonic()
        self.start_server()
        self.setup()
        threads = [
            threading.Thread(
                target=self.reconnecting_worker,
                args=(f"transfer-{worker}", self.transfer),
                daemon=True,
            )
            for worker in range(4)
        ]
        threads += [
            threading.Thread(
                target=self.reconnecting_worker,
                args=(f"reader-{worker}", self.indexed_read),
                daemon=True,
            )
            for worker in range(3)
        ]
        threads += [
            threading.Thread(
                target=self.reconnecting_worker,
                args=("atomic-epoch", self.atomic_epoch),
                daemon=True,
            ),
            threading.Thread(target=self.rewrite_loop, daemon=True),
            threading.Thread(target=self.invalid_rewrite_loop, daemon=True),
            threading.Thread(target=self.monitor, daemon=True),
        ]
        for thread in threads:
            thread.start()

        if self.args.crash_max_ms > 0:
            crash_thread = threading.Thread(target=self.random_crash_loop, daemon=True)
            threads.append(crash_thread)
            crash_thread.start()
        else:
            crash_at = [self.args.duration / 3, self.args.duration * 2 / 3]
            started = time.monotonic()
            for offset in crash_at:
                while not self.stop.is_set() and time.monotonic() - started < offset:
                    time.sleep(0.1)
                if not self.stop.is_set():
                    try:
                        self.crash_and_restart()
                    except Exception as error:
                        self.fail("crash-recovery", error)
        while not self.stop.is_set() and time.monotonic() < self.deadline:
            time.sleep(0.1)
        self.stop.set()
        for thread in threads:
            thread.join(timeout=35)
        if not self.errors:
            self.verify_durable_invariants("final")
        self.stop_server()
        self.report()
        return 1 if self.errors else 0

    def report(self):
        elapsed = time.monotonic() - self.started_at
        print(f"duration_s={elapsed:.1f} requested_s={self.args.duration}")
        for name, count in sorted(self.metrics.items()):
            print(f"count.{name}={count}")
        for name, values in sorted(self.latencies.items()):
            if not values:
                continue
            ordered = sorted(values)
            p95 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]
            print(
                f"latency_ms.{name}.median={statistics.median(values):.3f} "
                f"p95={p95:.3f} max={max(values):.3f}"
            )
        if self.samples:
            rss = [sample[2] for sample in self.samples]
            cpu = [sample[3] for sample in self.samples]
            sizes = [sample[6] for sample in self.samples]
            print(
                f"rss_mib.min={min(rss):.1f} median={statistics.median(rss):.1f} "
                f"max={max(rss):.1f}"
            )
            print(f"cpu_percent.mean={statistics.mean(cpu):.1f} max={max(cpu):.1f}")
            print(f"database_mib.min={min(sizes):.1f} max={max(sizes):.1f} final={sizes[-1]:.1f}")
            by_generation = collections.defaultdict(list)
            for sample in self.samples:
                by_generation[sample[1]].append(sample)
            io_samples = [sample for sample in self.samples if sample[4] >= 0]
            if io_samples:
                total_reads = 0
                total_writes = 0
                for generation, samples in by_generation.items():
                    available = [sample for sample in samples if sample[4] >= 0]
                    if available:
                        total_reads += max(sample[4] for sample in available) - min(
                            sample[4] for sample in available
                        )
                        total_writes += max(sample[5] for sample in available) - min(
                            sample[5] for sample in available
                        )
                print(f"process_block_io.read_ops={total_reads} write_ops={total_writes}")
            else:
                print("process_block_io=unavailable_on_host")
        for error in self.errors:
            print(f"ERROR {error}")
        print("verdict=PASS" if not self.errors else "verdict=FAIL")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="target/release/elyrasql")
    parser.add_argument("--data", required=True)
    parser.add_argument("--log", required=True)
    parser.add_argument("--port", type=int, default=33327)
    parser.add_argument("--duration", type=int, default=300)
    parser.add_argument("--crash-min-ms", type=int, default=0)
    parser.add_argument("--crash-max-ms", type=int, default=0)
    parser.add_argument("--crash-long-probability", type=float, default=0.0)
    parser.add_argument("--crash-long-max-ms", type=int, default=5000)
    parser.add_argument("--startup-crash-probability", type=float, default=0.0)
    parser.add_argument("--max-startup-crashes", type=int, default=2)
    parser.add_argument("--stop-before-kill-probability", type=float, default=0.0)
    parser.add_argument("--restart-attempts", type=int, default=1)
    args = parser.parse_args()
    if args.crash_min_ms < 0 or args.crash_max_ms < args.crash_min_ms:
        parser.error("crash interval must satisfy 0 <= min <= max")
    if args.crash_long_max_ms < args.crash_max_ms:
        parser.error("--crash-long-max-ms must be at least --crash-max-ms")
    if args.restart_attempts <= 0:
        parser.error("--restart-attempts must be positive")
    for name in (
        "crash_long_probability",
        "startup_crash_probability",
        "stop_before_kill_probability",
    ):
        if not 0 <= getattr(args, name) <= 1:
            parser.error(f"--{name.replace('_', '-')} must be between 0 and 1")
    campaign = Campaign(args)
    try:
        raise SystemExit(campaign.run())
    finally:
        campaign.stop.set()
        campaign.stop_server()


if __name__ == "__main__":
    main()
