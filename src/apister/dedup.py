"""Bounded on-disk log of recent message ids.

The log is the dedup ledger for at-least-once broker redelivery. Only the tail
is loaded, so memory stays capped while the file is compacted in place.
"""

from __future__ import annotations

import os
import threading
from collections import OrderedDict
from pathlib import Path

_ID_LEN = 16


class DedupLog:
    def __init__(self, path: Path, *, max_ids: int = 200_000) -> None:
        if max_ids < 1:
            raise ValueError("max_ids must be >= 1")
        self._path = path
        self._max = max_ids
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(path, "ab+")  # noqa: SIM115 — lifetime matches the bus
        self._file_lock = threading.Lock()
        self._seen: OrderedDict[bytes, None] = OrderedDict()
        self._buf = bytearray()
        self._load_tail()

    def remember(self, message_id: bytes) -> bool:
        """Record ``message_id``. Return ``False`` when it was already seen."""
        if len(message_id) != _ID_LEN:
            raise ValueError("message_id must be 16 bytes")
        with self._file_lock:
            if message_id in self._seen:
                self._seen.move_to_end(message_id)
                return False
            self._seen[message_id] = None
            self._buf += message_id
            while len(self._seen) > self._max:
                self._seen.popitem(last=False)
            should_flush = len(self._buf) >= 64 * 1024
        if should_flush:
            self.flush()
        return True

    def steal_pending(self) -> bytes:
        """Detach ids buffered on the event-loop thread. The caller fsyncs them."""
        with self._file_lock:
            if not self._buf:
                return b""
            data = bytes(self._buf)
            self._buf.clear()
            return data

    def write_pending(self, data: bytes) -> None:
        """Append ``data`` from the fsync thread and fsync the id log."""
        if not data:
            return
        with self._file_lock:
            self._fh.write(data)
            self._fh.flush()
            os.fsync(self._fh.fileno())
            if self._fh.tell() > self._max * _ID_LEN * 4:
                self._compact()

    def flush(self) -> None:
        with self._file_lock:
            if self._buf:
                self._fh.write(self._buf)
                self._buf.clear()
            self._fh.flush()
            if self._fh.tell() > self._max * _ID_LEN * 4:
                self._compact()

    def close(self) -> None:
        if self._fh.closed:
            return
        self.flush()
        self._fh.close()

    def _load_tail(self) -> None:
        size = self._path.stat().st_size
        start = max(0, size - self._max * _ID_LEN)
        start -= start % _ID_LEN
        with open(self._path, "rb") as handle:
            handle.seek(start)
            data = handle.read()
        usable = len(data) - (len(data) % _ID_LEN)
        for index in range(0, usable, _ID_LEN):
            message_id = data[index : index + _ID_LEN]
            self._seen[message_id] = None
            self._seen.move_to_end(message_id)
            while len(self._seen) > self._max:
                self._seen.popitem(last=False)

    def _compact(self) -> None:
        blob = b"".join(self._seen)
        self._fh.close()
        temporary = self._path.with_suffix(".tmp")
        temporary.write_bytes(blob)
        os.replace(temporary, self._path)
        self._fh = open(self._path, "ab+")  # noqa: SIM115
