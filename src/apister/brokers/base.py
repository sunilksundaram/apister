"""Broker adapter contract.

Adapters move bytes. Reconnect, dedup, and persistence stay in the bus.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol


class Incoming:
    """One message pulled from a broker. ``ack`` runs after it is durable."""

    __slots__ = ("topic", "payload", "ack")

    def __init__(self, topic: str, payload: bytes, ack: Ack) -> None:
        self.topic = topic
        self.payload = payload
        self.ack = ack


class Ack(Protocol):
    async def __call__(self) -> None: ...


class BrokerAdapter(Protocol):
    @property
    def name(self) -> str: ...

    async def connect(self) -> None: ...

    async def subscribe(self, patterns: Sequence[str]) -> None: ...

    async def publish(self, topic: str, payload: bytes) -> None: ...

    async def pull(self) -> Incoming: ...

    async def close(self) -> None: ...


async def ack_noop() -> None:
    return None
