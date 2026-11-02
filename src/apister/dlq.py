"""Dead Letter Queue (DLQ) and poison-pill containment mechanisms.

Provides structured metadata encapsulation, retry policies, and automated
routing to dead-letter topics when subscriber handlers encounter unrecoverable
errors or exceed retry quotas.
"""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from apister.bus import Bus
    from apister.types import Envelope


@dataclass(slots=True, frozen=True)
class DeadLetterRecord:
    """Structured record encapsulating a failed message and failure context."""

    original_topic: str
    original_offset: int
    message_id_hex: str
    error_reason: str
    exception_class: str
    retry_count: int
    failed_at_ns: int
    payload: bytes

    def to_dict(self) -> dict[str, Any]:
        """Convert record metadata and payload hex to dictionary."""
        return {
            "original_topic": self.original_topic,
            "original_offset": self.original_offset,
            "message_id": self.message_id_hex,
            "error_reason": self.error_reason,
            "exception_class": self.exception_class,
            "retry_count": self.retry_count,
            "failed_at_ns": self.failed_at_ns,
            "payload_hex": self.payload.hex(),
        }

    def to_bytes(self) -> bytes:
        """Serialize dead letter record to UTF-8 JSON bytes."""
        return json.dumps(self.to_dict()).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> DeadLetterRecord:
        """Deserialize dead letter record from UTF-8 JSON bytes."""
        doc = json.loads(data.decode("utf-8"))
        return cls(
            original_topic=doc["original_topic"],
            original_offset=doc["original_offset"],
            message_id_hex=doc["message_id"],
            error_reason=doc["error_reason"],
            exception_class=doc["exception_class"],
            retry_count=doc["retry_count"],
            failed_at_ns=doc["failed_at_ns"],
            payload=bytes.fromhex(doc["payload_hex"]),
        )


@dataclass(slots=True, frozen=True)
class DeadLetterPolicy:
    """Configuration governing retry limits and destination dead-letter topics."""

    max_retries: int = 3
    dlq_topic_suffix: str = ".dlq"
    retry_delay_seconds: float = 0.0

    def get_dlq_topic(self, topic: str) -> str:
        """Compute target DLQ topic name."""
        return f"{topic}{self.dlq_topic_suffix}"


async def route_to_dlq(
    bus: Bus,
    envelope: Envelope,
    exc: BaseException,
    retry_count: int = 0,
    policy: DeadLetterPolicy | None = None,
) -> Envelope | None:
    """Route a failed envelope to the designated dead-letter topic."""
    pol = policy or DeadLetterPolicy()
    dlq_topic = pol.get_dlq_topic(envelope.topic)

    record = DeadLetterRecord(
        original_topic=envelope.topic,
        original_offset=envelope.offset,
        message_id_hex=envelope.message_id.hex(),
        error_reason=str(exc),
        exception_class=f"{exc.__class__.__module__}.{exc.__class__.__name__}",
        retry_count=retry_count,
        failed_at_ns=time.time_ns(),
        payload=bytes(envelope.payload),
    )

    return await bus.publish(dlq_topic, record.to_bytes())


async def handle_with_dlq(
    bus: Bus,
    envelope: Envelope,
    handler: Callable[[Envelope], Awaitable[None]],
    policy: DeadLetterPolicy | None = None,
) -> bool:
    """Execute handler with retry policy and dead letter queue routing on failure.

    Returns True if successfully processed, False if routed to DLQ.
    """
    pol = policy or DeadLetterPolicy()
    retries = 0

    while True:
        try:
            await handler(envelope)
            return True
        except Exception as exc:  # noqa: BLE001
            retries += 1
            if retries > pol.max_retries:
                await route_to_dlq(bus, envelope, exc, retry_count=retries, policy=pol)
                return False
            if pol.retry_delay_seconds > 0:
                import asyncio

                await asyncio.sleep(pol.retry_delay_seconds)


__all__ = [
    "DeadLetterPolicy",
    "DeadLetterRecord",
    "handle_with_dlq",
    "route_to_dlq",
]
