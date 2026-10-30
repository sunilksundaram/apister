"""In-process broker used for tests and reconnect drills.

``fail_after`` successful publishes on a connection, the next publish raises
``ConnectionError`` until ``connect`` is called again.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from apister.brokers.base import Incoming, ack_noop
from apister.topics import topic_matches


class MemoryBroker:
    def __init__(self, *, fail_after: int | None = None, name: str = "memory") -> None:
        if fail_after is not None and fail_after < 1:
            raise ValueError("fail_after must be >= 1 or None")
        self._name = name
        self.fail_after = fail_after
        self.connects = 0
        self.sent: list[tuple[str, bytes]] = []
        self._connected = False
        self._since_connect = 0
        self._patterns: tuple[str, ...] = ()
        self._queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()

    @property
    def name(self) -> str:
        return self._name

    async def connect(self) -> None:
        self.connects += 1
        self._connected = True
        self._since_connect = 0

    async def subscribe(self, patterns: Sequence[str]) -> None:
        self._require()
        self._patterns = tuple(patterns)

    async def publish(self, topic: str, payload: bytes) -> None:
        self._require()
        if self.fail_after is not None and self._since_connect >= self.fail_after:
            self._connected = False
            raise ConnectionError("simulated broker drop")
        self._since_connect += 1
        self.sent.append((topic, payload))
        if any(topic_matches(pattern, topic) for pattern in self._patterns):
            await self._queue.put((topic, payload))

    async def pull(self) -> Incoming:
        self._require()
        topic, payload = await self._queue.get()
        if not self._connected:
            raise ConnectionError("simulated broker drop")
        return Incoming(topic, payload, ack_noop)

    async def close(self) -> None:
        self._connected = False

    def _require(self) -> None:
        if not self._connected:
            raise ConnectionError(f"{self._name} is not connected")
