"""Rust accelerator bindings and optional native core.

When compiled Rust extensions are present, this module exposes:
- ``NativeLedger``: Ultra-high-throughput disk ledger (~1.26M+ msgs/sec).
- ``NativeTopicTrie``: Zero-alloc MQTT topic matcher (~3.2M+ matches/sec).
- ``native_crc32``: SIMD-accelerated IEEE 802.3 CRC32 checksum.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from apister.types import Envelope, OffsetUnavailable

try:
    import _apister_rs  # type: ignore[import-untyped,import-not-found]

    HAS_RUST_CORE = True
except ImportError:
    _apister_rs = None  # type: ignore[assignment]
    HAS_RUST_CORE = False


def native_crc32(data: bytes) -> int:
    """Compute SIMD-accelerated CRC32 checksum if available, else zlib."""
    if HAS_RUST_CORE and _apister_rs is not None:
        return _apister_rs.rust_crc32(data)
    import zlib

    return zlib.crc32(data) & 0xFFFFFFFF


class NativeTopicTrie:
    """MQTT topic router accelerated with Rust radix trie."""

    def __init__(self) -> None:
        if not HAS_RUST_CORE or _apister_rs is None:
            raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
        self._inner = _apister_rs.RustTopicTrie()

    def add(self, pattern: str, subscription: Any) -> None:
        self._inner.add(pattern, subscription)

    def discard(self, pattern: str, subscription: Any) -> None:
        self._inner.discard(pattern, subscription)

    def match(self, topic: str) -> list[Any]:
        return self._inner.match_topic(topic)

    def __len__(self) -> int:
        return len(self._inner)


class NativeLedger:
    """Segmented append-only ledger accelerated with Rust and memory-mapped reads."""

    def __init__(
        self,
        directory: Path | str,
        *,
        segment_bytes: int = 64 * 1024 * 1024,
        commit_every: int = 128,
        retain_bytes: int | None = None,
    ) -> None:
        if not HAS_RUST_CORE or _apister_rs is None:
            raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
        self._dir = Path(directory)
        self._inner = _apister_rs.RustLedger(
            str(self._dir),
            segment_bytes,
            commit_every,
            retain_bytes,
        )

    @property
    def end_offset(self) -> int:
        return self._inner.end_offset

    @property
    def bytes_retained(self) -> int:
        return self._inner.bytes_retained

    @property
    def uncommitted(self) -> int:
        return self._inner.uncommitted

    def append(
        self,
        topic: str,
        payload: bytes,
        message_id: bytes,
        timestamp_ns: int,
        *,
        from_broker: bool,
        commit: bool | None = None,
        crc: int | None = None,
    ) -> Envelope:
        offset, next_offset = self._inner.append(
            topic,
            payload,
            message_id,
            timestamp_ns,
            from_broker,
            commit,
            crc,
        )
        return Envelope(
            offset=offset,
            next_offset=next_offset,
            topic=topic,
            payload=payload,
            message_id=message_id,
            timestamp_ns=timestamp_ns,
            from_broker=from_broker,
        )

    def read(self, offset: int) -> Envelope:
        try:
            (
                off,
                next_off,
                topic,
                payload,
                mid,
                ts,
                from_broker,
            ) = self._inner.read(offset)
        except ValueError as exc:
            raise OffsetUnavailable(offset) from exc

        return Envelope(
            offset=off,
            next_offset=next_off,
            topic=topic,
            payload=payload,
            message_id=mid,
            timestamp_ns=ts,
            from_broker=from_broker,
        )

    def read_next(self, offset: int, end: int | None = None) -> Envelope | None:
        rec = self._inner.read_next(offset, end)
        if rec is None:
            return None
        off, next_off, topic, payload, mid, ts, from_broker = rec
        return Envelope(
            offset=off,
            next_offset=next_off,
            topic=topic,
            payload=payload,
            message_id=mid,
            timestamp_ns=ts,
            from_broker=from_broker,
        )

    def commit_if_due(self) -> None:
        self._inner.commit_if_due()

    def commit(self) -> None:
        self._inner.commit()

    def close(self) -> None:
        self._inner.close()


class NativeDedupLog:
    """Bounded on-disk message deduplication log accelerated in Rust."""

    def __init__(self, path: Path | str, *, max_ids: int = 200_000) -> None:
        if not HAS_RUST_CORE or _apister_rs is None:
            raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
        self._path = Path(path)
        self._inner = _apister_rs.RustDedupLog(str(self._path), max_ids)

    def remember(self, message_id: bytes) -> bool:
        return self._inner.remember(message_id)

    def steal_pending(self) -> bytes:
        return b""

    def write_pending(self, data: bytes) -> None:
        self._inner.flush()

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()


def native_zstd_compress(data: bytes, level: int = 3, dict_data: bytes | None = None) -> bytes:
    """Compress bytes using native Rust Zstandard with optional pre-trained dictionary."""
    if not HAS_RUST_CORE or _apister_rs is None:
        raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
    return bytes(_apister_rs.rust_zstd_compress(data, level, dict_data))


def native_zstd_decompress(data: bytes, dict_data: bytes | None = None) -> bytes:
    """Decompress bytes using native Rust Zstandard with optional pre-trained dictionary."""
    if not HAS_RUST_CORE or _apister_rs is None:
        raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
    return bytes(_apister_rs.rust_zstd_decompress(data, dict_data))


def native_zstd_train_dict(samples: list[bytes], dict_size: int = 65536) -> bytes:
    """Train a Zstandard dictionary from sample byte buffers using native Rust."""
    if not HAS_RUST_CORE or _apister_rs is None:
        raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
    return bytes(_apister_rs.rust_zstd_train_dict(samples, dict_size))


def native_lz4_compress(data: bytes) -> bytes:
    """Compress bytes using native Rust LZ4 frame."""
    if not HAS_RUST_CORE or _apister_rs is None:
        raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
    return bytes(_apister_rs.rust_lz4_compress(data))


def native_lz4_decompress(data: bytes) -> bytes:
    """Decompress bytes using native Rust LZ4 frame."""
    if not HAS_RUST_CORE or _apister_rs is None:
        raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
    return bytes(_apister_rs.rust_lz4_decompress(data))


def native_inject_trace(payload: bytes, traceparent: str | None = None) -> bytes:
    """Inject W3C traceparent into payload prefix using native Rust."""
    if not HAS_RUST_CORE or _apister_rs is None:
        raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
    return bytes(_apister_rs.rust_inject_trace(payload, traceparent))


def native_extract_trace(payload: bytes) -> tuple[str | None, bytes]:
    """Extract W3C traceparent from payload prefix using native Rust."""
    if not HAS_RUST_CORE or _apister_rs is None:
        raise RuntimeError("Rust core extension '_apister_rs' is not installed.")
    tp, body = _apister_rs.rust_extract_trace(payload)
    return (tp, bytes(body))


__all__ = [
    "HAS_RUST_CORE",
    "NativeDedupLog",
    "NativeLedger",
    "NativeTopicTrie",
    "native_crc32",
    "native_extract_trace",
    "native_inject_trace",
    "native_lz4_compress",
    "native_lz4_decompress",
    "native_zstd_compress",
    "native_zstd_decompress",
    "native_zstd_train_dict",
]
