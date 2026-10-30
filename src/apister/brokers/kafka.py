"""Kafka adapter.

Requires the optional ``kafka`` extra (``aiokafka``). Topics are the bus topic
with ``/`` mapped to ``.``. The consumer uses one combined regex and commits
only after the bus has appended the record.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from apister.brokers.base import Incoming
from apister.brokers.patterns import dotted_to_topic, to_kafka_regex, topic_to_dotted
from apister.types import BrokerDependencyError

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
except ImportError:  # pragma: no cover - optional extra
    AIOKafkaConsumer = None  # type: ignore[assignment]
    AIOKafkaProducer = None  # type: ignore[assignment]


class KafkaBroker:
    def __init__(self, bootstrap_servers: str, *, group_id: str = "apister") -> None:
        self._servers = bootstrap_servers
        self._group_id = group_id
        self._producer: Any = None
        self._consumer: Any = None
        self._patterns: list[str] = []

    @property
    def name(self) -> str:
        return "kafka"

    async def connect(self) -> None:
        producer_cls, consumer_cls = _clients()
        self._producer = producer_cls(
            bootstrap_servers=self._servers,
            acks="all",
            enable_idempotence=True,
        )
        await self._producer.start()
        self._consumer = consumer_cls(
            bootstrap_servers=self._servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )

    async def subscribe(self, patterns: Sequence[str]) -> None:
        if self._consumer is None:
            raise ConnectionError("kafka is not connected")
        self._patterns = list(patterns)
        self._consumer.subscribe(pattern=to_kafka_regex(self._patterns))
        await self._consumer.start()

    async def publish(self, topic: str, payload: bytes) -> None:
        if self._producer is None:
            raise ConnectionError("kafka is not connected")
        await self._producer.send_and_wait(topic_to_dotted(topic), payload)

    async def pull(self) -> Incoming:
        if self._consumer is None:
            raise ConnectionError("kafka consumer is not started")
        message = await self._consumer.getone()
        topic = dotted_to_topic(message.topic)
        payload = bytes(message.value or b"")

        async def ack() -> None:
            await self._consumer.commit()

        return Incoming(topic, payload, ack)

    async def close(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None


def _clients() -> tuple[Any, Any]:
    if AIOKafkaProducer is None or AIOKafkaConsumer is None:
        raise BrokerDependencyError("Kafka support requires the optional dependency aiokafka")
    return AIOKafkaProducer, AIOKafkaConsumer
