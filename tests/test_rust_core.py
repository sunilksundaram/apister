import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from apister import Bus
from apister.native import (
    HAS_RUST_CORE,
    NativeDedupLog,
    NativeLedger,
    NativeTopicTrie,
    native_crc32,
)


class TestRustCore(unittest.IsolatedAsyncioTestCase):
    def test_native_crc32(self) -> None:
        data = b"testing native crc32 acceleration"
        crc = native_crc32(data)
        self.assertIsInstance(crc, int)
        self.assertGreater(crc, 0)

    @unittest.skipUnless(HAS_RUST_CORE, "Rust core extension not installed")
    def test_native_trie_matching(self) -> None:
        trie = NativeTopicTrie()
        trie.add("sensor/+/data", "sub_wildcard_plus")
        trie.add("sensor/#", "sub_wildcard_hash")
        trie.add("sensor/temp/data", "sub_exact")

        self.assertEqual(len(trie), 3)

        matches = trie.match("sensor/temp/data")
        self.assertEqual(set(matches), {"sub_wildcard_plus", "sub_wildcard_hash", "sub_exact"})

        matches_hash_only = trie.match("sensor/temp/nested/path")
        self.assertEqual(matches_hash_only, ["sub_wildcard_hash"])

        trie.discard("sensor/temp/data", "sub_exact")
        matches_after_discard = trie.match("sensor/temp/data")
        self.assertEqual(set(matches_after_discard), {"sub_wildcard_plus", "sub_wildcard_hash"})

    @unittest.skipUnless(HAS_RUST_CORE, "Rust core extension not installed")
    def test_native_ledger_append_and_read(self) -> None:
        with TemporaryDirectory() as raw:
            path = Path(raw)
            ledger = NativeLedger(path, segment_bytes=1024 * 1024, commit_every=10)

            records = []
            for i in range(25):
                rec = ledger.append(
                    f"topic/{i}",
                    f"payload_{i}".encode(),
                    b"mid1234567890123",
                    1000 + i,
                    from_broker=False,
                )
                records.append(rec)

            ledger.commit()
            self.assertEqual(ledger.end_offset, records[-1].next_offset)

            # Read back each record
            for orig in records:
                read_rec = ledger.read(orig.offset)
                self.assertEqual(read_rec.offset, orig.offset)
                self.assertEqual(read_rec.next_offset, orig.next_offset)
                self.assertEqual(read_rec.topic, orig.topic)
                self.assertEqual(read_rec.payload, orig.payload)
                self.assertEqual(read_rec.timestamp_ns, orig.timestamp_ns)

            ledger.close()

    @unittest.skipUnless(HAS_RUST_CORE, "Rust core extension not installed")
    def test_native_dedup_log(self) -> None:
        with TemporaryDirectory() as raw:
            path = Path(raw) / "dedup.log"
            dedup = NativeDedupLog(path, max_ids=100)

            mid1 = b"1234567890abcdef"
            mid2 = b"abcdef1234567890"

            self.assertTrue(dedup.remember(mid1))
            self.assertFalse(dedup.remember(mid1))  # duplicate
            self.assertTrue(dedup.remember(mid2))
            self.assertFalse(dedup.remember(mid2))  # duplicate

            dedup.flush()
            dedup.close()

            # Reopen and check persistence of remembered IDs
            reopened = NativeDedupLog(path, max_ids=100)
            self.assertFalse(reopened.remember(mid1))
            self.assertFalse(reopened.remember(mid2))
            reopened.close()

    async def test_bus_backend_and_publish_many(self) -> None:
        with TemporaryDirectory() as raw:
            path = Path(raw)
            # Test bus with auto backend (uses Rust core when available)
            async with await Bus.open(path, backend="auto") as bus:
                self.assertTrue(bus._use_rust)
                sub = await bus.subscribe("batch/+")

                batch = [
                    ("batch/item1", b"payload_1"),
                    ("batch/item2", b"payload_2"),
                    ("batch/item3", b"payload_3"),
                ]
                envelopes = await bus.publish_many(batch)
                self.assertEqual(len(envelopes), 3)
                self.assertIsNotNone(envelopes[0])

                # Verify zero-copy memoryview
                assert envelopes[0] is not None
                mv = envelopes[0].memoryview()
                self.assertEqual(bytes(mv), b"payload_1")

                # Receive via subscriber
                received = []
                for _ in range(3):
                    env = await asyncio.wait_for(sub.get(), timeout=1.0)
                    received.append(env.topic)
                    await sub.ack(env)

                self.assertEqual(received, ["batch/item1", "batch/item2", "batch/item3"])

        # Also test with explicit python backend
        with TemporaryDirectory() as raw2:
            path2 = Path(raw2)
            async with await Bus.open(path2, backend="python") as bus_py:
                self.assertFalse(bus_py._use_rust)
                sub = await bus_py.subscribe("batch/+")
                envelopes = await bus_py.publish_many([("batch/test", b"hello")])
                self.assertEqual(len(envelopes), 1)
                env = await asyncio.wait_for(sub.get(), timeout=1.0)
                self.assertEqual(env.payload, b"hello")


if __name__ == "__main__":
    unittest.main()
