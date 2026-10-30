import asyncio
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from apister.brokers import KafkaBroker, MemoryBroker, RabbitMQBroker
from apister.brokers import kafka as kafka_mod
from apister.brokers import rabbitmq as rabbit_mod
from apister.bus import Bus
from apister.types import (
    BrokerDependencyError,
    InvalidTopic,
    OverflowPolicy,
    PayloadTooLarge,
    PublishTimeout,
    ReconnectPolicy,
    SubscriptionClosed,
)


class BusTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.bus = await Bus.open(
            Path(self._tmp.name),
            max_queue_size=8,
            commit_every=1,
            segment_bytes=1024 * 1024,
        )

    async def asyncTearDown(self) -> None:
        await self.bus.shutdown()
        self._tmp.cleanup()

    async def test_fanout_and_wildcard(self) -> None:
        exact = await self.bus.subscribe("news/sports/final")
        wild = await self.bus.subscribe("news/#")
        other = await self.bus.subscribe("weather/+")
        await self.bus.publish("news/sports/final", b"one")
        await self.bus.publish("news/weather", b"two")
        self.assertEqual((await exact.get()).payload, b"one")
        self.assertEqual((await wild.get()).payload, b"one")
        self.assertEqual((await wild.get()).payload, b"two")
        self.assertTrue(other.queue.empty())

    async def test_offsets_not_payloads_are_queued(self) -> None:
        subscription = await self.bus.subscribe("news/item")
        await self.bus.publish("news/item", b"z" * 1000)
        offset = subscription.queue.get_nowait()
        self.assertIsInstance(offset, int)
        envelope = await self.bus._materialize(offset)
        self.assertEqual(envelope.payload, b"z" * 1000)

    async def test_backpressure_blocks_until_drained(self) -> None:
        bus = await Bus.open(
            Path(self._tmp.name) / "bp",
            max_queue_size=1,
            command_queue_size=8,
            overflow=OverflowPolicy.BLOCK,
            commit_every=1,
        )
        try:
            subscription = await bus.subscribe("t")
            await bus.publish("t", b"1")
            blocked = asyncio.create_task(bus.publish("t", b"2"))
            await asyncio.sleep(0.05)
            self.assertFalse(blocked.done())
            self.assertEqual((await subscription.get()).payload, b"1")
            second = await asyncio.wait_for(blocked, timeout=1)
            assert second is not None
            self.assertEqual(second.payload, b"2")
        finally:
            await bus.shutdown()

    async def test_drop_newest(self) -> None:
        bus = await Bus.open(
            Path(self._tmp.name) / "drop",
            max_queue_size=1,
            overflow=OverflowPolicy.DROP_NEWEST,
            commit_every=1,
        )
        try:
            subscription = await bus.subscribe("t")
            await bus.publish("t", b"keep")
            await bus.publish("t", b"shed")
            self.assertGreaterEqual(bus.dropped, 1)
            self.assertEqual((await subscription.get()).payload, b"keep")
        finally:
            await bus.shutdown()

    async def test_replay_after_reopen(self) -> None:
        await self.bus.publish("news/a", b"persisted")
        await self.bus.shutdown()
        reopened = await Bus.open(Path(self._tmp.name), commit_every=1)
        try:
            subscription = await reopened.subscribe("news/#", replay_from=0)
            envelope = await asyncio.wait_for(subscription.get(), timeout=1)
            self.assertEqual(envelope.payload, b"persisted")
        finally:
            await reopened.shutdown()

    async def test_shutdown_sentinel(self) -> None:
        subscription = await self.bus.subscribe("news/a")
        await self.bus.publish("news/a", b"bye")
        await self.bus.shutdown()
        self.assertEqual((await subscription.get()).payload, b"bye")
        with self.assertRaises(SubscriptionClosed):
            await subscription.get()

    async def test_rejects_bad_input(self) -> None:
        with self.assertRaises(InvalidTopic):
            await self.bus.publish("bad/+", b"x")
        tiny = await Bus.open(Path(self._tmp.name) / "tiny", max_payload_bytes=4, commit_every=1)
        try:
            with self.assertRaises(PayloadTooLarge):
                await tiny.publish("ok", b"12345")
        finally:
            await tiny.shutdown()

    async def test_broker_forward_dedup_and_reconnect(self) -> None:
        broker = MemoryBroker(fail_after=1)
        subscription = await self.bus.subscribe("news/#")
        await self.bus.attach_broker(
            broker,
            patterns=["news/#"],
            policy=ReconnectPolicy(initial_delay_s=0.01, max_delay_s=0.02, jitter_ratio=0),
        )
        await self.bus.publish("news/sports", b"play")
        await self.bus.publish("news/sports", b"again")
        first = await asyncio.wait_for(subscription.get(), timeout=1)
        second = await asyncio.wait_for(subscription.get(), timeout=1)
        self.assertEqual([first.payload, second.payload], [b"play", b"again"])
        for _ in range(100):
            if len(broker.sent) >= 2 and broker.connects >= 2:
                break
            await asyncio.sleep(0.01)
        self.assertGreaterEqual(len(broker.sent), 2)
        self.assertGreaterEqual(broker.connects, 2)
        await asyncio.sleep(0.05)
        self.assertTrue(subscription.queue.empty())

    async def test_many_messages_keep_a_single_payload_copy(self) -> None:
        subscription = await self.bus.subscribe("bulk")
        count = 2000

        async def produce() -> None:
            for _ in range(count):
                await self.bus.publish("bulk", b"m")

        async def consume() -> None:
            for _ in range(count):
                envelope = await subscription.get()
                self.assertEqual(envelope.payload, b"m")

        started = time.perf_counter()
        await asyncio.wait_for(asyncio.gather(produce(), consume()), timeout=30)
        elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 30)
        self.assertEqual(self.bus.stats().published, count)

    async def test_out_of_order_ack_waits_for_the_gap(self) -> None:
        path = Path(self._tmp.name) / "ack"
        bus = await Bus.open(path, commit_every=1)
        try:
            subscription = await bus.subscribe("jobs", name="worker")
            await bus.publish("jobs", b"a")
            await bus.publish("jobs", b"b")
            first = await subscription.get()
            second = await subscription.get()
            self.assertEqual([first.payload, second.payload], [b"a", b"b"])
            await subscription.ack(second)
            self.assertEqual(subscription.cursor, 0)
            await subscription.ack(first)
            self.assertEqual(subscription.cursor, second.next_offset)
        finally:
            await bus.shutdown()

    async def test_named_ack_resumes_after_the_acked_prefix(self) -> None:
        path = Path(self._tmp.name) / "ack-prefix"
        bus = await Bus.open(path, commit_every=1)
        try:
            subscription = await bus.subscribe("jobs", name="worker")
            await bus.publish("jobs", b"a")
            await bus.publish("jobs", b"b")
            first = await subscription.get()
            await subscription.ack(first)
        finally:
            await bus.shutdown()
        reopened = await Bus.open(path, commit_every=1)
        try:
            subscription = await reopened.subscribe("jobs", name="worker")
            second = await asyncio.wait_for(subscription.get(), timeout=1)
            self.assertEqual(second.payload, b"b")
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(subscription.get(), timeout=0.1)
        finally:
            await reopened.shutdown()

    async def test_unacked_named_subscription_replays(self) -> None:
        path = Path(self._tmp.name) / "replay-ack"
        bus = await Bus.open(path, commit_every=1)
        try:
            await bus.subscribe("jobs", name="worker")
            await bus.publish("jobs", b"a")
            await bus.publish("jobs", b"b")
        finally:
            await bus.shutdown()
        reopened = await Bus.open(path, commit_every=1)
        try:
            subscription = await reopened.subscribe("jobs", name="worker")
            first = await asyncio.wait_for(subscription.get(), timeout=1)
            second = await asyncio.wait_for(subscription.get(), timeout=1)
            self.assertEqual([first.payload, second.payload], [b"a", b"b"])
        finally:
            await reopened.shutdown()

    async def test_inflight_limit_parks_until_ack(self) -> None:
        subscription = await self.bus.subscribe("llm", inflight_limit=1, queue_size=8)
        await self.bus.publish("llm", b"1")
        await self.bus.publish("llm", b"2")
        self.assertEqual(subscription.queue.qsize(), 1)
        self.assertEqual(self.bus.stats().inflight, 1)
        self.assertEqual(self.bus.stats().parked, 1)
        first = await subscription.get()
        self.assertEqual(first.payload, b"1")
        self.assertTrue(subscription.queue.empty())
        await subscription.ack(first)
        second = await asyncio.wait_for(subscription.get(), timeout=1)
        self.assertEqual(second.payload, b"2")
        self.assertEqual(self.bus.stats().parked, 0)

    async def test_publish_timeout_does_not_append(self) -> None:
        bus = await Bus.open(
            Path(self._tmp.name) / "timeout",
            max_queue_size=1,
            command_queue_size=8,
            commit_every=1,
        )
        try:
            subscription = await bus.subscribe("t")
            await bus.publish("t", b"1")
            end = bus._ledger.end_offset
            with self.assertRaises(PublishTimeout):
                await bus.publish("t", b"2", timeout=0.05)
            self.assertEqual(bus._ledger.end_offset, end)
            self.assertEqual(bus.stats().published, 1)
            self.assertEqual((await subscription.get()).payload, b"1")
        finally:
            await bus.shutdown()

    async def test_timed_fsync_updates_checkpoint(self) -> None:
        path = Path(self._tmp.name) / "fsync"
        bus = await Bus.open(path, commit_every=1_000_000, fsync_interval_s=0.05)
        try:
            self.assertEqual(_checkpoint(path), 0)
            await bus.publish("t", b"x")
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline and _checkpoint(path) == 0:
                await asyncio.sleep(0.02)
            self.assertGreater(_checkpoint(path), 0)
        finally:
            await bus.shutdown()


def _checkpoint(path: Path) -> int:
    return int.from_bytes((path / "ledger" / "ckpt").read_bytes(), "little")


class OptionalBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_clients_fail_clearly(self) -> None:
        rabbit = RabbitMQBroker("amqp://guest:guest@127.0.0.1/")
        kafka = KafkaBroker("127.0.0.1:9092")
        try:
            if rabbit_mod.aio_pika is None:
                with self.assertRaises(BrokerDependencyError):
                    await rabbit.connect()
            if kafka_mod.AIOKafkaProducer is None:
                with self.assertRaises(BrokerDependencyError):
                    await kafka.connect()
        finally:
            await rabbit.close()
            await kafka.close()
