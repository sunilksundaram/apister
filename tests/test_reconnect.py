import unittest

from apister.brokers import MemoryBroker
from apister.reconnect import ReconnectingBroker
from apister.types import BrokerUnavailable, ReconnectPolicy


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_publish_reconnects_after_simulated_drop(self) -> None:
        broker = MemoryBroker(fail_after=1)
        session = ReconnectingBroker(
            broker,
            ReconnectPolicy(initial_delay_s=0.01, max_delay_s=0.02, jitter_ratio=0, max_attempts=6),
            ["news/#"],
            self._ignore,
        )
        await session.start()
        try:
            for index in range(4):
                await session.publish("news/item", f"m{index}".encode())
        finally:
            await session.stop()
        self.assertGreaterEqual(broker.connects, 2)
        self.assertEqual(len(broker.sent), 4)
        self.assertGreaterEqual(session.reconnects, 1)

    async def test_gives_up_at_max_attempts(self) -> None:
        broker = MemoryBroker(fail_after=1)
        session = ReconnectingBroker(
            broker,
            ReconnectPolicy(
                initial_delay_s=0.001, max_delay_s=0.001, jitter_ratio=0, max_attempts=1
            ),
            ["news/#"],
            self._ignore,
        )
        await session.start()
        try:
            await session.publish("news/item", b"once")
            with self.assertRaises(BrokerUnavailable):
                await session.publish("news/item", b"twice")
        finally:
            await session.stop()

    async def _ignore(self, _incoming: object) -> None:
        return None
