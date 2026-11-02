"""Benchmark comparing Python Ledger vs RustLedger and Python Trie vs RustTopicTrie."""

from __future__ import annotations

import gc
import shutil
import tempfile
import time
from pathlib import Path

import _apister_rs

from apister.ledger import Ledger as PyLedger
from apister.topics import TopicTrie as PyTrie


def benchmark_ledgers() -> None:
    tmp_root = Path(tempfile.mkdtemp(prefix="apister_bench_cmp_"))
    payload = b"x" * 1024
    topic = "bench/comparison"
    mid = b"1234567890abcdef"
    n_records = 50_000

    try:
        # 1. Benchmark Pure Append: Python
        py_dir = tmp_root / "py_ledger"
        py_ledger = PyLedger(py_dir, segment_bytes=128 * 1024 * 1024, commit_every=10_000)
        gc.collect()
        t0 = time.perf_counter()
        for i in range(n_records):
            py_ledger.append(topic, payload, mid, i, from_broker=False, commit=False)
        py_ledger.commit()
        t1 = time.perf_counter()
        py_dur = t1 - t0
        py_rate = n_records / py_dur
        py_ledger.close()

        # 2. Benchmark Pure Append: Rust
        rust_dir = tmp_root / "rust_ledger"
        rust_ledger = _apister_rs.RustLedger(str(rust_dir), 128 * 1024 * 1024, 10_000)
        gc.collect()
        t0 = time.perf_counter()
        for i in range(n_records):
            rust_ledger.append(topic, payload, mid, i, False, False, None)
        rust_ledger.commit()
        t1 = time.perf_counter()
        rust_dur = t1 - t0
        rust_rate = n_records / rust_dur
        rust_ledger.close()

        # 3. Benchmark Topic Trie
        py_trie = PyTrie()
        rust_trie = _apister_rs.RustTopicTrie()

        patterns = [
            "telemetry/+/imu",
            "telemetry/#",
            "telemetry/drone42/+",
            "alerts/+/critical",
            "market/crypto/+/swaps",
            "market/#",
            "orders/executable",
        ]

        for p in patterns:
            py_trie.add(p, f"sub_{p}")
            rust_trie.add(p, f"sub_{p}")

        match_topic = "telemetry/drone42/imu"
        n_matches = 100_000

        # Python Trie match
        gc.collect()
        t0 = time.perf_counter()
        for _ in range(n_matches):
            py_trie.match(match_topic)
        t1 = time.perf_counter()
        py_trie_rate = n_matches / (t1 - t0)

        # Rust Trie match
        gc.collect()
        t0 = time.perf_counter()
        for _ in range(n_matches):
            rust_trie.match_topic(match_topic)
        t1 = time.perf_counter()
        rust_trie_rate = n_matches / (t1 - t0)

        # 4. Benchmark CRC32
        import zlib

        data = b"benchmark payload" * 64
        n_crc = 200_000

        t0 = time.perf_counter()
        for _ in range(n_crc):
            zlib.crc32(data)
        t1 = time.perf_counter()
        py_crc_rate = n_crc / (t1 - t0)

        t0 = time.perf_counter()
        for _ in range(n_crc):
            _apister_rs.rust_crc32(data)
        t1 = time.perf_counter()
        rust_crc_rate = n_crc / (t1 - t0)

        print("=" * 68)
        print("PYTHON vs RUST PERFORMANCE BENCHMARK (50k records, 1KB payloads)")
        print("=" * 68)
        print(f"Append Throughput (Python): {py_rate:>12,.1f} msgs/sec ({py_dur:.3f}s)")
        print(f"Append Throughput (Rust)  : {rust_rate:>12,.1f} msgs/sec ({rust_dur:.3f}s)")
        print(f"  -> Speedup: {rust_rate / py_rate:.2f}x")
        print("-" * 68)
        print(f"Topic Trie Match (Python) : {py_trie_rate:>12,.1f} matches/sec")
        print(f"Topic Trie Match (Rust)   : {rust_trie_rate:>12,.1f} matches/sec")
        print(f"  -> Speedup: {rust_trie_rate / py_trie_rate:.2f}x")
        print("-" * 68)
        print(f"CRC32 Hardware (Python)   : {py_crc_rate:>12,.1f} checks/sec")
        print(f"CRC32 Hardware (Rust)     : {rust_crc_rate:>12,.1f} checks/sec")
        print(f"  -> Speedup: {rust_crc_rate / py_crc_rate:.2f}x")
        print("=" * 68)

    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    benchmark_ledgers()
