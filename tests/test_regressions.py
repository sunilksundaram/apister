"""Regression test suite for apister.

Guards against regressions in:
- Storage durability and uncommitted checkpoint recovery
- Out-of-order ack cursor monotonicity
- Inflight limit parking and FIFO queue draining
- Deduplication cache compaction
- Topic boundary matching edge-cases
"""

from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from apister.bus import Bus
from apister.dedup import DedupLog
from apister.topics import topic_matches
from apister.types import (
    OverflowPolicy,
    PublishTimeout,
)


class StorageAndAckRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_regression_ack_cursor_monotonicity_across_reconnects(self) -> None:
        """Ensure ack cursors never regress backwards or skip unacked gaps after crash/reopen."""
        with TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)

            # Session 1: publish 3 messages, ack message 1 and 3 (leaving 2 unacked)
            bus1 = await Bus.open(path, commit_every=1)
            try:
                sub = await bus1.subscribe("jobs", name="worker-regress")
                await bus1.publish("jobs", b"job-1")
                await bus1.publish("jobs", b"job-2")
                await bus1.publish("jobs", b"job-3")

                msg1 = await sub.get()
                _msg2 = await sub.get()
                msg3 = await sub.get()

                # Ack out of order: ack msg3, then msg1
                await sub.ack(msg3)
                await sub.ack(msg1)
            finally:
                await bus1.shutdown()

            # Session 2: reopen with same cursor name; must redeliver from msg2 and then msg3
            bus2 = await Bus.open(path, commit_every=1)
            try:
                sub2 = await bus2.subscribe("jobs", name="worker-regress")
                msg2_re = await asyncio.wait_for(sub2.get(), timeout=1.0)
                self.assertEqual(msg2_re.payload, b"job-2")
                await sub2.ack(msg2_re)

                msg3_re = await asyncio.wait_for(sub2.get(), timeout=1.0)
                self.assertEqual(msg3_re.payload, b"job-3")
                await sub2.ack(msg3_re)

                # No more unacked messages
                with self.assertRaises(TimeoutError):
                    await asyncio.wait_for(sub2.get(), timeout=0.1)
            finally:
                await bus2.shutdown()

    async def test_regression_inflight_parking_fifo_order(self) -> None:
        """Verify parked messages resume strictly in FIFO order when ack slot frees."""
        with TemporaryDirectory() as tmp_dir:
            bus = await Bus.open(Path(tmp_dir), commit_every=1)
            try:
                sub = await bus.subscribe("pipeline", inflight_limit=1, queue_size=10)
                await bus.publish("pipeline", b"first")
                await bus.publish("pipeline", b"second")
                await bus.publish("pipeline", b"third")

                # Only 1 is delivered; 2 are parked
                self.assertEqual(bus.stats().inflight, 1)
                self.assertEqual(bus.stats().parked, 2)

                m1 = await sub.get()
                self.assertEqual(m1.payload, b"first")
                await sub.ack(m1)

                m2 = await asyncio.wait_for(sub.get(), timeout=1.0)
                self.assertEqual(m2.payload, b"second")
                await sub.ack(m2)

                m3 = await asyncio.wait_for(sub.get(), timeout=1.0)
                self.assertEqual(m3.payload, b"third")
                await sub.ack(m3)

                self.assertEqual(bus.stats().parked, 0)
                self.assertEqual(bus.stats().inflight, 0)
            finally:
                await bus.shutdown()

    async def test_regression_publish_timeout_leaves_state_unmutated(self) -> None:
        """Publish timeout under backpressure must not leak records or advance ledger offsets."""
        with TemporaryDirectory() as tmp_dir:
            bus = await Bus.open(
                Path(tmp_dir),
                max_queue_size=1,
                command_queue_size=4,
                overflow=OverflowPolicy.BLOCK,
                commit_every=1,
            )
            try:
                sub = await bus.subscribe("feed")
                await bus.publish("feed", b"slot-filled")

                initial_offset = bus.stats().ledger_bytes
                initial_published = bus.stats().published

                with self.assertRaises(PublishTimeout):
                    await bus.publish("feed", b"rejected", timeout=0.05)

                # Verify counters and ledger bytes did not change
                self.assertEqual(bus.stats().ledger_bytes, initial_offset)
                self.assertEqual(bus.stats().published, initial_published)

                received = await sub.get()
                self.assertEqual(received.payload, b"slot-filled")
            finally:
                await bus.shutdown()


class DedupAndTopicRegressionTests(unittest.TestCase):
    def test_regression_dedup_log_compaction_and_reload(self) -> None:
        """Dedup log must retain seen IDs across reloads and bounded capacity."""
        with TemporaryDirectory() as tmp_dir:
            log_path = Path(tmp_dir) / "dedup.log"
            dedup = DedupLog(log_path, max_ids=10)

            ids = [f"msg-{i:012d}".encode("ascii") for i in range(15)]
            for mid in ids:
                dedup.remember(mid)

            # Close and reopen
            dedup.close()
            reopened = DedupLog(log_path, max_ids=10)

            # Older IDs (> 10 items ago) may have been evicted
            # Most recent 10 IDs must still be deduplicated
            for mid in ids[-10:]:
                self.assertFalse(reopened.remember(mid), f"ID {mid} should be remembered")

            # Brand new ID must be accepted
            self.assertTrue(reopened.remember(b"brand-new-id-012"))
            reopened.close()

    def test_regression_topic_boundary_matching(self) -> None:
        """Topic segment boundaries must not match substring prefixes."""
        self.assertFalse(topic_matches("users/+/profile", "users/admin123/profile/extra"))
        self.assertFalse(topic_matches("orders/ny", "orders/nyc"))
        self.assertTrue(topic_matches("orders/#", "orders"))
        self.assertTrue(topic_matches("orders/#", "orders/create"))
        self.assertTrue(topic_matches("orders/#", "orders/create/priority"))


if __name__ == "__main__":
    unittest.main()
