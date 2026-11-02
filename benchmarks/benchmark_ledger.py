"""Comprehensive Ledger Benchmark Suite for Apister.

Measures:
1. Pure Sequential Append Throughput (single segment, buffered)
2. Append Throughput with Rotations & Periodic Commits (multi-segment)
3. Segment Locate Latency/Throughput (_locate across many segments)
4. Sequential Replay Throughput (reading from offset 0 across sealed segments)
5. Random Access Read Throughput (across sealed segments)
6. Active Segment Read Throughput
"""

from __future__ import annotations

import gc
import random
import shutil
import tempfile
import time
from pathlib import Path

from apister.ledger import Ledger


def run_benchmarks() -> dict[str, float]:
    results: dict[str, float] = {}
    tmp_root = Path(tempfile.mkdtemp(prefix="apister_bench_"))

    try:
        # ---------------------------------------------------------
        # Benchmark 1: Pure Sequential Append (1 KB payloads)
        # ---------------------------------------------------------
        dir_b1 = tmp_root / "b1"
        ledger1 = Ledger(dir_b1, segment_bytes=128 * 1024 * 1024, commit_every=10_000)
        payload_1k = b"x" * 1024
        mid = b"0123456789abcdef"
        n_b1 = 60_000

        gc.collect()
        t0 = time.perf_counter()
        for i in range(n_b1):
            ledger1.append("bench/pure_append", payload_1k, mid, i, from_broker=False, commit=False)
        ledger1.commit()
        t1 = time.perf_counter()
        ledger1.close()

        dur_b1 = t1 - t0
        rate_b1 = n_b1 / dur_b1
        mb_b1 = (n_b1 * (1024 + 44 + len("bench/pure_append"))) / (1024 * 1024) / dur_b1
        results["pure_append_msgs_per_sec"] = rate_b1
        results["pure_append_mb_per_sec"] = mb_b1

        # ---------------------------------------------------------
        # Benchmark 2: Append with Rotations & Periodic Commits
        # ---------------------------------------------------------
        dir_b2 = tmp_root / "b2"
        # 2 MB segments -> ~15-20 segment rotations for 30,000 records
        ledger2 = Ledger(dir_b2, segment_bytes=2 * 1024 * 1024, commit_every=128)
        n_b2 = 30_000
        offsets_b2: list[int] = []

        gc.collect()
        t0 = time.perf_counter()
        for i in range(n_b2):
            env = ledger2.append("bench/multi_segment", payload_1k, mid, i, from_broker=False)
            offsets_b2.append(env.offset)
        ledger2.commit()
        t1 = time.perf_counter()

        dur_b2 = t1 - t0
        rate_b2 = n_b2 / dur_b2
        results["rotation_append_msgs_per_sec"] = rate_b2
        results["segments_created"] = float(len(ledger2._segments))

        # ---------------------------------------------------------
        # Benchmark 3: Segment Locate Throughput (_locate across segments)
        # ---------------------------------------------------------
        # Sample mix: 70% active segment, 30% random historical segments
        active_off = offsets_b2[-100]
        historical_sample = [
            random.choice(offsets_b2[: -len(offsets_b2) // 4]) for _ in range(5_000)
        ]
        locate_offsets = ([active_off] * 15_000) + historical_sample
        random.shuffle(locate_offsets)

        t0 = time.perf_counter()
        for off in locate_offsets:
            ledger2._locate(off)
        t1 = time.perf_counter()
        dur_b3 = t1 - t0
        results["locate_lookups_per_sec"] = len(locate_offsets) / dur_b3

        # ---------------------------------------------------------
        # Benchmark 4: Sequential Replay from Offset 0
        # ---------------------------------------------------------
        # Replay first 10,000 records across multiple sealed segments
        replay_count = 10_000
        gc.collect()
        t0 = time.perf_counter()
        for off in offsets_b2[:replay_count]:
            ledger2.read(off)
        t1 = time.perf_counter()
        dur_b4 = t1 - t0
        results["sequential_replay_reads_per_sec"] = replay_count / dur_b4

        # ---------------------------------------------------------
        # Benchmark 5: Random Access Reads Across Sealed Segments
        # ---------------------------------------------------------
        sealed_offsets = [off for off in offsets_b2 if off < ledger2._active_base]
        random_sample = [random.choice(sealed_offsets) for _ in range(10_000)]

        gc.collect()
        t0 = time.perf_counter()
        for off in random_sample:
            ledger2.read(off)
        t1 = time.perf_counter()
        dur_b5 = t1 - t0
        results["random_sealed_reads_per_sec"] = len(random_sample) / dur_b5

        # ---------------------------------------------------------
        # Benchmark 6: Active Segment Reads
        # ---------------------------------------------------------
        # Ensure active segment is readable
        ledger2.commit()
        active_offsets = [off for off in offsets_b2 if off >= ledger2._active_base]
        active_sample = [
            random.choice(active_offsets) for _ in range(min(5_000, len(active_offsets)))
        ]

        gc.collect()
        t0 = time.perf_counter()
        for off in active_sample:
            ledger2.read(off)
        t1 = time.perf_counter()
        dur_b6 = t1 - t0
        results["active_reads_per_sec"] = len(active_sample) / dur_b6

        ledger2.close()

    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)

    return results


if __name__ == "__main__":
    res = run_benchmarks()
    print("=" * 60)
    print("APISTER LEDGER BENCHMARK RESULTS")
    print("=" * 60)
    for k, v in res.items():
        if "per_sec" in k:
            print(f"{k:<35}: {v:>12,.1f} ops/sec")
        else:
            print(f"{k:<35}: {v:>12,.1f}")
    print("=" * 60)
