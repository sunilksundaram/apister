"""Segmented append-only ledger.

Each message is written once. Subscribers receive the byte offset, so fan-out
does not copy payloads. Segments are sealed and trusted after fsync; only the
active segment is scanned on open, from the last committed checkpoint.
"""

from __future__ import annotations

import os
import struct
import threading
import zlib
from pathlib import Path
from typing import Any

from apister.types import Envelope, LedgerCorrupt, OffsetUnavailable

_MAGIC = b"AP1\n"
_HEADER = struct.Struct("<4sHHQQ16sI")
_HEADER_SIZE = _HEADER.size
_FROM_BROKER = 1
_SEAL = struct.Struct("<Q")


class Ledger:
    """Single-writer log. Reads may run on another thread; a lock covers IO."""

    def __init__(
        self,
        directory: Path,
        *,
        segment_bytes: int = 64 * 1024 * 1024,
        commit_every: int = 128,
        retain_bytes: int | None = None,
    ) -> None:
        if segment_bytes < _HEADER_SIZE:
            raise ValueError("segment_bytes is too small")
        if commit_every < 1:
            raise ValueError("commit_every must be >= 1")
        if retain_bytes is not None and retain_bytes < 1:
            raise ValueError("retain_bytes must be >= 1 or None")
        self._dir = directory
        self._segment_bytes = segment_bytes
        self._commit_every = commit_every
        self._retain_bytes = retain_bytes
        self._io = threading.Lock()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._segments: list[tuple[int, int, Path]] = []
        self._reads: dict[Path, Any] = {}
        self._end_offset = 0
        self._active_base = 0
        self._active_size = 0
        self._active_path = directory / f"{0:016x}.seg"
        self._fh: Any = None
        self._since_commit = 0
        self._recover()

    @property
    def end_offset(self) -> int:
        return self._end_offset

    @property
    def bytes_retained(self) -> int:
        with self._io:
            if not self._segments and self._end_offset == self._active_base:
                return self._active_size
            start = self._segments[0][0] if self._segments else self._active_base
            return self._end_offset - start

    def append(
        self,
        topic: str,
        payload: bytes,
        message_id: bytes,
        timestamp_ns: int,
        *,
        from_broker: bool,
        commit: bool | None = None,
    ) -> Envelope:
        """Append one record.

        ``commit=False`` skips the ``commit_every`` fsync. ``commit=True`` fsyncs
        this record immediately. ``None`` fsyncs when ``commit_every`` is reached.
        """
        topic_b = topic.encode("utf-8")
        flags = _FROM_BROKER if from_broker else 0
        crc = zlib.crc32(topic_b)
        crc = zlib.crc32(payload, crc) & 0xFFFFFFFF
        header = _HEADER.pack(
            _MAGIC,
            len(topic_b),
            flags,
            len(payload),
            timestamp_ns,
            message_id,
            crc,
        )
        with self._io:
            if self._active_size >= self._segment_bytes and self._active_size > 0:
                self._rotate()
            offset = self._end_offset
            handle = self._require_handle()
            handle.write(header)
            handle.write(topic_b)
            handle.write(payload)
            size = _HEADER_SIZE + len(topic_b) + len(payload)
            self._active_size += size
            self._end_offset = offset + size
            self._since_commit += 1
            if commit is True or (commit is None and self._since_commit >= self._commit_every):
                self._commit()
            self._enforce_retention()
            return Envelope(
                offset=offset,
                next_offset=offset + size,
                topic=topic,
                payload=payload,
                message_id=message_id,
                timestamp_ns=timestamp_ns,
                from_broker=from_broker,
            )

    def read(self, offset: int) -> Envelope:
        with self._io:
            return self._read_at(offset, self._end_offset)

    def read_next(self, offset: int, end: int | None = None) -> Envelope | None:
        """Return the record at ``offset`` if it is strictly before ``end``."""
        with self._io:
            limit = self._end_offset if end is None else min(end, self._end_offset)
            if offset >= limit:
                return None
            return self._read_at(offset, limit)

    @property
    def uncommitted(self) -> int:
        """Records appended since the last fsync."""
        return self._since_commit

    def commit_if_due(self) -> None:
        """Fsync when ``commit_every`` records are waiting."""
        with self._io:
            if self._since_commit >= self._commit_every:
                self._commit()

    def commit(self) -> None:
        with self._io:
            self._commit()

    def close(self) -> None:
        with self._io:
            if self._fh is None:
                return
            self._commit()
            self._require_handle().close()
            self._fh = None
            for handle in self._reads.values():
                handle.close()  # type: ignore[attr-defined]
            self._reads.clear()

    def _commit(self) -> None:
        handle = self._require_handle()
        handle.flush()
        os.fsync(handle.fileno())
        self._write_ckpt(self._end_offset)
        self._since_commit = 0

    def _recover(self) -> None:
        paths = sorted(self._dir.glob("*.seg"))
        if not paths:
            self._open_active(0)
            self._write_ckpt(0)
            return
        expected = 0
        for path in paths[:-1]:
            base = _base_of(path)
            if base != expected:
                raise LedgerCorrupt(f"segment gap at {path.name}")
            end = self._sealed_end(path, base)
            self._segments.append((base, end, path))
            expected = end
        active = paths[-1]
        base = _base_of(active)
        if base != expected:
            raise LedgerCorrupt(f"active segment gap at {active.name}")
        end = self._walk(active, base, self._ckpt_local(base))
        self._end_offset = end
        self._open_active(base)
        self._active_size = end - base
        if self._active_path.stat().st_size != self._active_size:
            self._require_handle().truncate(self._active_size)
            self._require_handle().flush()

    def _sealed_end(self, path: Path, base: int) -> int:
        seal = path.with_suffix(".end")
        if seal.exists():
            data = seal.read_bytes()
            if len(data) != _SEAL.size:
                raise LedgerCorrupt(f"bad seal {seal.name}")
            return _SEAL.unpack(data)[0]
        end = self._walk(path, base, 0)
        self._write_seal(path, end)
        return end

    def _walk(self, path: Path, base: int, start_local: int) -> int:
        size = path.stat().st_size
        if start_local < 0 or start_local > size:
            start_local = 0
        good = start_local
        with open(path, "rb") as handle:
            handle.seek(start_local)
            while True:
                local = handle.tell()
                header = handle.read(_HEADER_SIZE)
                if not header:
                    break
                if len(header) < _HEADER_SIZE:
                    break
                try:
                    magic, topic_len, _flags, payload_len, _ts, _mid, crc = _HEADER.unpack(header)
                except struct.error:
                    break
                if magic != _MAGIC or topic_len > 1024:
                    if local + _HEADER_SIZE > size:
                        break
                    raise LedgerCorrupt(f"bad record header in {path.name} at {local}")
                body_len = topic_len + payload_len
                if local + _HEADER_SIZE + body_len > size:
                    break
                body = handle.read(body_len)
                if len(body) < body_len:
                    break
                actual = zlib.crc32(body) & 0xFFFFFFFF
                if actual != crc:
                    raise LedgerCorrupt(f"crc mismatch in {path.name} at {local}")
                good = local + _HEADER_SIZE + body_len
        if good < size:
            with open(path, "r+b") as handle:
                handle.truncate(good)
                handle.flush()
                os.fsync(handle.fileno())
        return base + good

    def _ckpt_local(self, base: int) -> int:
        path = self._dir / "ckpt"
        if not path.exists():
            return 0
        data = path.read_bytes()
        if len(data) != 8:
            return 0
        ckpt = int.from_bytes(data, "little")
        if ckpt < base:
            return 0
        return ckpt - base

    def _open_active(self, base: int) -> None:
        path = self._dir / f"{base:016x}.seg"
        self._active_path = path
        self._active_base = base
        self._fh = open(path, "ab+")  # noqa: SIM115
        self._active_size = path.stat().st_size

    def _rotate(self) -> None:
        handle = self._require_handle()
        handle.flush()
        os.fsync(handle.fileno())
        end = self._end_offset
        self._write_seal(self._active_path, end)
        handle.close()
        self._close_read(self._active_path)
        self._segments.append((self._active_base, end, self._active_path))
        self._open_active(end)
        self._active_size = 0

    def _enforce_retention(self) -> None:
        if self._retain_bytes is None:
            return
        horizon = self._end_offset - self._retain_bytes
        while self._segments and self._segments[0][1] <= horizon:
            _base, _end, path = self._segments.pop(0)
            self._close_read(path)
            path.unlink(missing_ok=True)
            path.with_suffix(".end").unlink(missing_ok=True)

    def _read_at(self, offset: int, limit: int) -> Envelope:
        if offset < 0 or offset >= limit:
            raise OffsetUnavailable(offset)
        path, local = self._locate(offset)
        handle = self._reader(path)
        handle.seek(local)  # type: ignore[attr-defined]
        header = handle.read(_HEADER_SIZE)  # type: ignore[attr-defined]
        if len(header) < _HEADER_SIZE:
            raise LedgerCorrupt(f"short header at {offset}")
        magic, topic_len, flags, payload_len, timestamp_ns, message_id, crc = _HEADER.unpack(header)
        if magic != _MAGIC:
            raise LedgerCorrupt(f"bad magic at {offset}")
        body = handle.read(topic_len + payload_len)  # type: ignore[attr-defined]
        if len(body) < topic_len + payload_len:
            raise LedgerCorrupt(f"short body at {offset}")
        if (zlib.crc32(body) & 0xFFFFFFFF) != crc:
            raise LedgerCorrupt(f"crc mismatch at {offset}")
        next_offset = offset + _HEADER_SIZE + topic_len + payload_len
        if next_offset > limit:
            raise OffsetUnavailable(offset)
        topic = body[:topic_len].decode("utf-8")
        payload = bytes(body[topic_len:])
        return Envelope(
            offset=offset,
            next_offset=next_offset,
            topic=topic,
            payload=payload,
            message_id=bytes(message_id),
            timestamp_ns=timestamp_ns,
            from_broker=bool(flags & _FROM_BROKER),
        )

    def _locate(self, offset: int) -> tuple[Path, int]:
        for base, end, path in self._segments:
            if base <= offset < end:
                return path, offset - base
        if self._active_base <= offset < self._end_offset:
            return self._active_path, offset - self._active_base
        raise OffsetUnavailable(offset)

    def _reader(self, path: Path) -> Any:
        handle = self._reads.get(path)
        if handle is None:
            handle = open(path, "rb")  # noqa: SIM115
            self._reads[path] = handle
        return handle

    def _close_read(self, path: Path) -> None:
        handle = self._reads.pop(path, None)
        if handle is not None:
            handle.close()

    def _require_handle(self) -> Any:
        if self._fh is None:
            raise LedgerCorrupt("ledger is closed")
        return self._fh

    def _write_ckpt(self, offset: int) -> None:
        temporary = self._dir / "ckpt.tmp"
        temporary.write_bytes(offset.to_bytes(8, "little"))
        os.replace(temporary, self._dir / "ckpt")

    def _write_seal(self, path: Path, end: int) -> None:
        seal = path.with_suffix(".end")
        temporary = path.with_suffix(".end.tmp")
        temporary.write_bytes(_SEAL.pack(end))
        os.replace(temporary, seal)


def _base_of(path: Path) -> int:
    try:
        return int(path.stem, 16)
    except ValueError as exc:
        raise LedgerCorrupt(f"bad segment name {path.name}") from exc
