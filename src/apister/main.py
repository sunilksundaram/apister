"""CLI entrypoint: demo runner, offline ledger inspector, and CRC integrity verifier."""

from __future__ import annotations

import asyncio
import os
import struct
import sys
import tempfile
import zlib
from pathlib import Path

from apister.brokers import MemoryBroker
from apister.bus import Bus
from apister.types import OverflowPolicy, ReconnectPolicy, SubscriptionClosed

_MAGIC = b"AP1\n"
_HEADER = struct.Struct("<4sHHQQ16sI")
_HEADER_SIZE = _HEADER.size


def inspect_directory(root_path: Path | str) -> int:
    """Print structural diagnostics of an apister storage directory."""
    root = Path(root_path)
    if not root.exists():
        print(f"Error: Directory '{root}' does not exist.", file=sys.stderr)
        return 1

    print(f"apister Storage Inspection: {root.resolve()}")
    print("-" * 60)

    # 1. Instance ID
    instance_file = root / "instance-id"
    if instance_file.exists():
        iid = instance_file.read_bytes().hex()
        print(f"Instance ID:       {iid}")
    else:
        print("Instance ID:       <not initialized>")

    # 2. Ledger Segments
    ledger_dir = root / "ledger"
    if ledger_dir.exists():
        ckpt_file = ledger_dir / "ckpt"
        ckpt_offset = None
        if ckpt_file.exists():
            with open(ckpt_file, "rb") as f:
                if f.seek(0, os.SEEK_END) >= 8:
                    f.seek(0)
                    (ckpt_offset,) = struct.unpack("<Q", f.read(8))
        print(f"Checkpoint Offset: {ckpt_offset if ckpt_offset is not None else '<none>'} (ckpt)")

        segments = sorted(ledger_dir.glob("*.seg"))
        total_bytes = sum(s.stat().st_size for s in segments)
        print(f"Total Segments:    {len(segments)} ({total_bytes / (1024 * 1024):.2f} MB)")
        for seg in segments:
            base_hex = seg.stem
            size_kb = seg.stat().st_size / 1024
            print(f"  - {seg.name}  (base: 0x{base_hex}, size: {size_kb:.1f} KB)")
    else:
        print("Ledger Directory:  <not found>")

    # 3. Persistent Cursors
    cursors_dir = root / "cursors"
    if cursors_dir.exists():
        cursors = sorted(cursors_dir.glob("*"))
        named = [c for c in cursors if not c.name.endswith(".tmp")]
        print(f"\nPersistent Cursors ({len(named)}):")
        for c in named:
            try:
                val = int(c.read_text(encoding="utf-8").strip())
                print(f"  - {c.name:20s} -> offset {val}")
            except Exception as exc:
                print(f"  - {c.name:20s} -> <error: {exc}>")
    else:
        print("\nPersistent Cursors: None")

    # 4. Bridge Outboxes
    bridges_dir = root / "bridges"
    if bridges_dir.exists():
        bridges = sorted(bridges_dir.glob("*.cursor"))
        print(f"\nBridge Outboxes ({len(bridges)}):")
        for b in bridges:
            try:
                val = int(b.read_text(encoding="utf-8").strip())
                print(f"  - {b.stem:20s} -> offset {val}")
            except Exception as exc:
                print(f"  - {b.stem:20s} -> <error: {exc}>")

    # 5. Deduplication Log
    dedup_file = root / "dedup"
    if dedup_file.exists():
        dedup_size = dedup_file.stat().st_size
        print(f"\nDeduplication Log: {dedup_size} bytes (~{dedup_size // 16} message IDs)")

    return 0


def verify_directory(root_path: Path | str) -> int:
    """Verify IEEE 802.3 CRC32 integrity across all ledger segments on disk."""
    root = Path(root_path)
    ledger_dir = root / "ledger" if (root / "ledger").exists() else root
    segments = sorted(ledger_dir.glob("*.seg"))

    if not segments:
        print(f"No .seg segment files found in '{ledger_dir}'.", file=sys.stderr)
        return 1

    print(f"Verifying {len(segments)} segment file(s) in {ledger_dir}...")
    corrupted = 0
    total_records = 0

    for idx, seg in enumerate(segments, 1):
        file_size = seg.stat().st_size
        records_in_seg = 0
        seg_corrupt = False

        with open(seg, "rb") as fh:
            offset_in_seg = 0
            while offset_in_seg < file_size:
                header_data = fh.read(_HEADER_SIZE)
                if len(header_data) < _HEADER_SIZE:
                    if len(header_data) > 0:
                        print(
                            f"  [!] {seg.name}: trailing {len(header_data)} unparsed bytes at offset {offset_in_seg}"
                        )
                    break

                magic, topic_len, _flags, payload_len, _ts, _mid, expected_crc = _HEADER.unpack(
                    header_data
                )
                if magic != _MAGIC:
                    print(
                        f"  [ERROR] {seg.name}: invalid magic {magic!r} at offset {offset_in_seg}"
                    )
                    seg_corrupt = True
                    break

                topic_bytes = fh.read(topic_len)
                payload_bytes = fh.read(payload_len)

                if len(topic_bytes) != topic_len or len(payload_bytes) != payload_len:
                    print(
                        f"  [ERROR] {seg.name}: unexpected EOF while reading record at {offset_in_seg}"
                    )
                    seg_corrupt = True
                    break

                # CRC32 verification
                actual_crc = zlib.crc32(topic_bytes)
                actual_crc = zlib.crc32(payload_bytes, actual_crc) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    print(
                        f"  [CRC MISMATCH] {seg.name}: offset {offset_in_seg} (expected 0x{expected_crc:08x}, got 0x{actual_crc:08x})"
                    )
                    seg_corrupt = True
                    break

                offset_in_seg += _HEADER_SIZE + topic_len + payload_len
                records_in_seg += 1

        total_records += records_in_seg
        if seg_corrupt:
            corrupted += 1
            print(f"  [{idx}/{len(segments)}] {seg.name}: CORRUPTED")
        else:
            print(
                f"  [{idx}/{len(segments)}] {seg.name}: {records_in_seg} records, {file_size} bytes ... OK"
            )

    print("-" * 60)
    if corrupted > 0:
        print(f"FAILED: {corrupted} segment(s) failed integrity validation.")
        return 1

    print(
        f"PASSED: All {len(segments)} segment(s) verified ({total_records} valid records). Zero corruption detected."
    )
    return 0


async def run_demo() -> None:
    root = Path(tempfile.mkdtemp(prefix="apister-demo-"))
    broker = MemoryBroker(fail_after=1)
    policy = ReconnectPolicy(initial_delay_s=0.01, max_delay_s=0.05, jitter_ratio=0)

    print(f"Running apister interactive demo at {root}...")
    async with await Bus.open(
        root,
        max_queue_size=8,
        overflow=OverflowPolicy.BLOCK,
        segment_bytes=64 * 1024,
        commit_every=1,
    ) as bus:
        print(f"Bus opened (backend={bus.stats().backend}, healthy={bus.is_healthy})")
        exact = await bus.subscribe("news/sports/final")
        wild = await bus.subscribe("news/#")
        await bus.attach_broker(broker, patterns=["news/#"], policy=policy)

        # Publish batch
        await bus.publish_many(
            [
                ("news/sports/final", b"game over"),
                ("news/weather", b"clear"),
            ]
        )

        for subscription, expected in (
            (exact, [b"game over"]),
            (wild, [b"game over", b"clear"]),
        ):
            for _payload in expected:
                envelope = await subscription.get()
                print(f"  {subscription.pattern:20s} <- [{envelope.topic}] {envelope.payload!r}")

        for _ in range(100):
            if len(broker.sent) >= 2 and broker.connects >= 2:
                break
            await asyncio.sleep(0.01)
        print(f"  broker status: connects={broker.connects}, forwarded={len(broker.sent)}")

        await bus.unsubscribe(exact)
        try:
            await exact.get()
        except SubscriptionClosed:
            print("  exact subscriber cleanly stopped on SHUTDOWN sentinel")

    reopened = await Bus.open(root, commit_every=1)
    try:
        replay = await reopened.subscribe("news/#", replay_from=0)
        seen = 0
        while seen < 2:
            envelope = await asyncio.wait_for(replay.get(), timeout=2)
            seen += 1
            print(f"  replay (offset {envelope.offset}): {envelope.payload!r}")
        print(f"Ledger verified: {reopened.stats().ledger_bytes} bytes stored.")
    finally:
        await reopened.shutdown()
        print("Demo completed successfully.")


def cli_entrypoint() -> None:
    args = sys.argv[1:]
    if not args or args[0] in ("demo", "--demo"):
        asyncio.run(run_demo())
        return

    cmd = args[0].lower()
    if cmd == "inspect":
        if len(args) < 2:
            print("Usage: apister inspect <path_to_bus_directory>", file=sys.stderr)
            sys.exit(1)
        sys.exit(inspect_directory(args[1]))
    elif cmd == "verify":
        if len(args) < 2:
            print("Usage: apister verify <path_to_bus_directory>", file=sys.stderr)
            sys.exit(1)
        sys.exit(verify_directory(args[1]))
    elif cmd in ("-h", "--help", "help"):
        print("apister - High-fanout in-process pub/sub bus with durable ledger\n")
        print("Commands:")
        print("  demo                Run the interactive pub/sub demo")
        print("  inspect <dir>       Inspect ledger segments, cursors, and checkpoints")
        print("  verify  <dir>       Validate IEEE 802.3 CRC32 checksums of all records")
        sys.exit(0)
    else:
        print(f"Unknown command: {cmd!r}. Run 'apister --help' for usage.")
        sys.exit(1)


# Compatibility alias for [project.scripts] and python -m apister
main = cli_entrypoint


if __name__ == "__main__":
    cli_entrypoint()
