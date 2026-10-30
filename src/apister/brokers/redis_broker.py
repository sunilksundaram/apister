"""Dependency-free Redis pub/sub adapter.

Uses two connections: RESP publish commands cannot share a subscribed socket.
Install nothing extra. ``rediss://`` enables TLS.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any
from urllib.parse import unquote, urlsplit

from apister.brokers.base import Incoming, ack_noop
from apister.brokers.patterns import to_redis_globs


class RedisBroker:
    def __init__(self, url: str = "redis://127.0.0.1:6379/0") -> None:
        parts = urlsplit(url)
        if parts.scheme not in {"redis", "rediss"}:
            raise ValueError("Redis url must start with redis:// or rediss://")
        self._host = parts.hostname or "127.0.0.1"
        self._port = parts.port or 6379
        self._ssl = parts.scheme == "rediss"
        self._password = unquote(parts.password) if parts.password else None
        self._db = int(parts.path.lstrip("/") or "0")
        self._pub: _Resp | None = None
        self._sub: _Resp | None = None

    @property
    def name(self) -> str:
        return "redis"

    async def connect(self) -> None:
        self._pub = await _Resp.connect(self._host, self._port, self._ssl, self._password, self._db)
        self._sub = await _Resp.connect(self._host, self._port, self._ssl, self._password, self._db)

    async def subscribe(self, patterns: Sequence[str]) -> None:
        exact: list[str] = []
        globs: list[str] = []
        for pattern in patterns:
            channels, pattern_globs = to_redis_globs(pattern)
            exact.extend(channels)
            globs.extend(pattern_globs)
        sub = self._require_sub()
        if exact:
            await sub.command("SUBSCRIBE", *exact)
        if globs:
            await sub.command("PSUBSCRIBE", *globs)

    async def publish(self, topic: str, payload: bytes) -> None:
        await self._require_pub().command("PUBLISH", topic, payload)

    async def pull(self) -> Incoming:
        sub = self._require_sub()
        while True:
            message = await sub.read()
            if not isinstance(message, list) or len(message) < 3:
                continue
            kind = message[0]
            if kind == b"message" and len(message) >= 3:
                channel = _text(message[1])
                body = _bytes(message[2])
                return Incoming(channel, body, ack_noop)
            if kind == b"pmessage" and len(message) >= 4:
                channel = _text(message[2])
                body = _bytes(message[3])
                return Incoming(channel, body, ack_noop)

    async def close(self) -> None:
        for conn in (self._pub, self._sub):
            if conn is not None:
                await conn.close()
        self._pub = None
        self._sub = None

    def _require_pub(self) -> _Resp:
        if self._pub is None:
            raise ConnectionError("redis publish connection is closed")
        return self._pub

    def _require_sub(self) -> _Resp:
        if self._sub is None:
            raise ConnectionError("redis subscribe connection is closed")
        return self._sub


class _Resp:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        ssl: bool,
        password: str | None,
        db: int,
    ) -> _Resp:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ssl),
            timeout=10,
        )
        conn = cls(reader, writer)
        if password is not None:
            await conn.command("AUTH", password)
        if db:
            await conn.command("SELECT", str(db))
        return conn

    async def command(self, *parts: str | bytes) -> Any:
        encoded = [_bulk(part) for part in parts]
        header = f"*{len(encoded)}\r\n".encode()
        self._writer.write(header + b"".join(encoded))
        await self._writer.drain()
        return await self.read()

    async def read(self) -> Any:
        line = await self._reader.readline()
        if not line:
            raise ConnectionError("redis closed the connection")
        kind = line[:1]
        body = line[1:-2]
        if kind == b"+":
            return body
        if kind == b"-":
            raise ConnectionError(body.decode("utf-8", "replace"))
        if kind == b":":
            return int(body)
        if kind == b"$":
            length = int(body)
            if length < 0:
                return None
            data = await self._reader.readexactly(length + 2)
            return data[:-2]
        if kind == b"*":
            count = int(body)
            if count < 0:
                return None
            return [await self.read() for _ in range(count)]
        raise ConnectionError(f"unexpected RESP prefix {kind!r}")

    async def close(self) -> None:
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except (ConnectionError, OSError, RuntimeError):
            return


def _bulk(part: str | bytes) -> bytes:
    data = part.encode("utf-8") if isinstance(part, str) else part
    return f"${len(data)}\r\n".encode() + data + b"\r\n"


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise ConnectionError("redis payload was not bulk data")
