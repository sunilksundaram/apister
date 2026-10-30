"""RabbitMQ topic-exchange adapter.

Requires the optional ``rabbitmq`` extra (``aio-pika``). The bus supervisor
owns reconnect, so this adapter uses a single connection rather than
``connect_robust``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from apister.brokers.base import Incoming
from apister.brokers.patterns import dotted_to_topic, to_amqp_routing, topic_to_dotted
from apister.types import BrokerDependencyError

try:
    import aio_pika
except ImportError:  # pragma: no cover - optional extra
    aio_pika = None


class RabbitMQBroker:
    def __init__(self, url: str, *, exchange: str = "apister") -> None:
        self._url = url
        self._exchange_name = exchange
        self._connection: Any = None
        self._exchange: Any = None
        self._queue: Any = None
        self._iterator: Any = None
        self._iterator_cm: Any = None

    @property
    def name(self) -> str:
        return "rabbitmq"

    async def connect(self) -> None:
        client = _client()
        self._connection = await client.connect(self._url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=128)
        self._exchange = await channel.declare_exchange(
            self._exchange_name,
            client.ExchangeType.TOPIC,
            durable=True,
        )
        queue = await channel.declare_queue(exclusive=True)
        self._queue = queue

    async def subscribe(self, patterns: Sequence[str]) -> None:
        if self._exchange is None:
            raise ConnectionError("rabbitmq is not connected")
        for pattern in patterns:
            await self._queue.bind(self._exchange, routing_key=to_amqp_routing(pattern))
        self._iterator_cm = self._queue.iterator()
        self._iterator = await self._iterator_cm.__aenter__()

    async def publish(self, topic: str, payload: bytes) -> None:
        client = _client()
        if self._exchange is None:
            raise ConnectionError("rabbitmq is not connected")
        message = client.Message(body=payload, delivery_mode=client.DeliveryMode.PERSISTENT)
        await self._exchange.publish(message, routing_key=topic_to_dotted(topic))

    async def pull(self) -> Incoming:
        if self._iterator is None:
            raise ConnectionError("rabbitmq consumer is not subscribed")
        try:
            message = await self._iterator.__anext__()
        except StopAsyncIteration as exc:
            raise ConnectionError("rabbitmq consumer ended") from exc
        topic = dotted_to_topic(message.routing_key)
        payload = bytes(message.body)

        async def ack() -> None:
            await message.ack()

        return Incoming(topic, payload, ack)

    async def close(self) -> None:
        if self._iterator_cm is not None:
            await self._iterator_cm.__aexit__(None, None, None)
            self._iterator_cm = None
            self._iterator = None
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._exchange = None
            self._queue = None


def _client() -> Any:
    if aio_pika is None:
        raise BrokerDependencyError("RabbitMQ support requires the optional dependency aio-pika")
    return aio_pika
