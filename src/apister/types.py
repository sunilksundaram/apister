"""Shared types for the in-process bus."""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Never


class ApisterError(Exception):
    """Base error for the bus."""


class BusClosed(ApisterError):
    """Publish or subscribe was attempted after shutdown."""


class SubscriptionClosed(ApisterError):
    """The subscription has received the shutdown sentinel."""


class InvalidTopic(ApisterError):
    """A topic or subscription pattern is not valid."""


class PayloadTooLarge(ApisterError):
    """A payload exceeds the configured maximum."""


class PublishTimeout(ApisterError):
    """``publish`` waited longer than ``timeout`` for subscriber queue space."""

    def __init__(self, topic: str) -> None:
        super().__init__(f"publish to {topic!r} timed out waiting for queue space")
        self.topic = topic


class LedgerCorrupt(ApisterError):
    """A ledger segment failed validation."""


class OffsetUnavailable(ApisterError):
    """The requested ledger offset is outside the retained log."""


class BrokerUnavailable(ApisterError):
    """An external broker could not be reached within the retry budget."""


class BrokerDependencyError(ApisterError):
    """An optional broker client library is not installed."""


class OverflowPolicy(enum.Enum):
    """What to do when a subscriber queue is full."""

    BLOCK = "block"
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"


class _ShutdownSentinel:
    """Value placed on subscriber queues to end iteration."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "SHUTDOWN"


SHUTDOWN = _ShutdownSentinel()


@dataclass(frozen=True, slots=True)
class Envelope:
    """One durable message. Fan-out queues store ``offset``, not this object."""

    offset: int
    next_offset: int
    topic: str
    payload: bytes
    message_id: bytes
    timestamp_ns: int
    from_broker: bool


@dataclass(frozen=True, slots=True)
class BusStats:
    """Point-in-time counters. Safe to read from the event-loop thread."""

    published: int
    delivered: int
    dropped: int
    broker_forwards: int
    broker_ingests: int
    duplicates: int
    reconnects: int
    subscribers: int
    ledger_bytes: int
    inflight: int
    parked: int


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    """Exponential backoff with jitter for broker sessions.

    ``max_attempts`` counts failures after the initial connect. ``None`` retries
    until the bus shuts down.
    """

    initial_delay_s: float = 0.05
    max_delay_s: float = 30.0
    multiplier: float = 2.0
    jitter_ratio: float = 0.2
    max_attempts: int | None = None

    def __post_init__(self) -> None:
        if self.initial_delay_s < 0 or self.max_delay_s < 0:
            raise ValueError("reconnect delays must be >= 0")
        if self.multiplier < 1:
            raise ValueError("reconnect multiplier must be >= 1")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1 or None")


def unreachable(value: Never) -> Never:
    """Fail compilation when a match misses a union member."""
    raise AssertionError(f"unreachable: {value!r}")
