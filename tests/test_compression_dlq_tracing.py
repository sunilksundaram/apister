"""Tests for compression, DLQ, distributed tracing, and disk space warning."""

from __future__ import annotations

import asyncio
import tempfile
import unittest

from apister.bus import Bus
from apister.compression import (
    GzipCodec,
    LZ4Codec,
    NoopCodec,
    ZstdCodec,
    pack_compressed,
    train_zstd_dictionary,
    unpack_compressed,
)
from apister.dlq import (
    DeadLetterPolicy,
    DeadLetterRecord,
    handle_with_dlq,
)
from apister.native import HAS_RUST_CORE
from apister.tracing import (
    extract_trace_context,
    generate_traceparent,
    inject_trace_context,
    parse_traceparent,
)


class TestCompression(unittest.TestCase):
    def test_noop_codec(self) -> None:
        codec = NoopCodec()
        self.assertEqual(codec.name, "none")
        payload = b"hello world 123"
        compressed = codec.compress(payload)
        self.assertEqual(compressed, payload)
        self.assertEqual(codec.decompress(compressed), payload)

    def test_gzip_codec(self) -> None:
        codec = GzipCodec()
        self.assertEqual(codec.name, "gzip")
        payload = b"sensor-data-chunk-repeated-" * 50
        compressed = codec.compress(payload)
        self.assertLess(len(compressed), len(payload))
        self.assertEqual(codec.decompress(compressed), payload)

    def test_lz4_codec(self) -> None:
        codec = LZ4Codec()
        self.assertEqual(codec.name, "lz4")
        payload = b"telemetry-sensor-frame-data-sample-sequence-" * 40
        compressed = codec.compress(payload)
        self.assertLess(len(compressed), len(payload))
        self.assertEqual(codec.decompress(compressed), payload)

    def test_zstd_codec(self) -> None:
        codec = ZstdCodec(level=3)
        self.assertEqual(codec.name, "zstd")
        payload = b'{"sensor_id": "temp_01", "reading": 23.4, "unit": "celsius"}' * 25
        compressed = codec.compress(payload)
        self.assertLess(len(compressed), len(payload))
        self.assertEqual(codec.decompress(compressed), payload)

    def test_zstd_dictionary_training_and_usage(self) -> None:
        samples = [
            f'{{"id": {i}, "device": "alpha", "temp": 20.{i}, "status": "active"}}'.encode()
            for i in range(100)
        ]
        dict_data = train_zstd_dictionary(samples, dict_size=16384)
        self.assertGreater(len(dict_data), 0)

        # Compress with dictionary
        codec = ZstdCodec(level=3, dict_data=dict_data)
        test_sample = b'{"id": 42, "device": "alpha", "temp": 20.42, "status": "active"}'
        compressed = codec.compress(test_sample)
        # Decompress with dictionary
        decompressed = codec.decompress(compressed)
        self.assertEqual(decompressed, test_sample)

    def test_pack_and_unpack_compressed(self) -> None:
        data = b"message payload with structured tabular telemetry" * 10
        packed_zstd = pack_compressed(data, "zstd")
        self.assertEqual(unpack_compressed(packed_zstd), data)

        packed_lz4 = pack_compressed(data, "lz4")
        self.assertEqual(unpack_compressed(packed_lz4), data)

        packed_gzip = pack_compressed(data, "gzip")
        self.assertEqual(unpack_compressed(packed_gzip), data)

        packed_raw = pack_compressed(data, "none")
        self.assertEqual(unpack_compressed(packed_raw), data)


class TestTracing(unittest.TestCase):
    def test_generate_and_parse_traceparent(self) -> None:
        tp = generate_traceparent()
        self.assertEqual(len(tp), 55)
        parsed = parse_traceparent(tp)
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["version"], "00")
        self.assertEqual(len(parsed["trace_id"]), 32)
        self.assertEqual(len(parsed["parent_id"]), 16)
        self.assertTrue(parsed["sampled"])

    def test_invalid_traceparent(self) -> None:
        self.assertIsNone(parse_traceparent("invalid-header"))
        self.assertIsNone(
            parse_traceparent("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        )

    def test_inject_and_extract_trace_context(self) -> None:
        payload = b'{"action": "order.submit", "account": 12345}'
        tp = generate_traceparent()
        injected = inject_trace_context(payload, tp)
        extracted_tp, body = extract_trace_context(injected)
        self.assertEqual(extracted_tp, tp)
        self.assertEqual(body, payload)

    def test_extract_trace_without_header(self) -> None:
        payload = b"raw plain payload without any trace header"
        extracted_tp, body = extract_trace_context(payload)
        self.assertIsNone(extracted_tp)
        self.assertEqual(body, payload)


class TestDLQAndBus(unittest.IsolatedAsyncioTestCase):
    async def test_dead_letter_record_roundtrip(self) -> None:
        rec = DeadLetterRecord(
            original_topic="orders/us",
            original_offset=42,
            message_id_hex="abcdef0123456789",
            error_reason="Division by zero",
            exception_class="builtins.ZeroDivisionError",
            retry_count=3,
            failed_at_ns=1234567890,
            payload=b"raw-order-bytes",
        )
        rec_bytes = rec.to_bytes()
        restored = DeadLetterRecord.from_bytes(rec_bytes)
        self.assertEqual(restored.original_topic, rec.original_topic)
        self.assertEqual(restored.original_offset, rec.original_offset)
        self.assertEqual(restored.message_id_hex, rec.message_id_hex)
        self.assertEqual(restored.error_reason, rec.error_reason)
        self.assertEqual(restored.payload, rec.payload)

    async def test_bus_stats_disk_and_dlq_routing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bus = await Bus.open(
                td,
                backend="auto",
                disk_warning_threshold=0.99,
            )
            stats = bus.stats()
            self.assertGreater(stats.disk_free_bytes, 0)
            self.assertGreaterEqual(stats.disk_usage_percent, 0.0)
            self.assertFalse(stats.disk_warning)
            if HAS_RUST_CORE:
                self.assertEqual(stats.backend, "rust")
            else:
                self.assertEqual(stats.backend, "python")

            # Test subscribe with cursor alias
            cursor_sub = await bus.subscribe("orders/asia", cursor="asia-worker")
            self.assertEqual(cursor_sub.name, "asia-worker")
            await bus.unsubscribe(cursor_sub)

            # Test DLQ routing
            dlq_sub = await bus.subscribe("orders/eu.dlq")
            env = await bus.publish("orders/eu", b"bad json payload")
            self.assertIsNotNone(env)
            assert env is not None

            # Test handle_with_dlq
            policy = DeadLetterPolicy(max_retries=2, retry_delay_seconds=0.0)
            attempt_count = 0

            async def faulty_handler(_item: object) -> None:
                nonlocal attempt_count
                attempt_count += 1
                raise ValueError("JSON decode failure: corrupted token")

            success = await handle_with_dlq(bus, env, faulty_handler, policy=policy)
            self.assertFalse(success)
            self.assertEqual(attempt_count, 3)  # initial try + 2 retries

            # Verify DLQ received the envelope
            dlq_msg = await asyncio.wait_for(dlq_sub.get(), timeout=2.0)
            dlq_rec = DeadLetterRecord.from_bytes(dlq_msg.payload)
            self.assertEqual(dlq_rec.original_topic, "orders/eu")
            self.assertEqual(dlq_rec.payload, b"bad json payload")
            self.assertIn("JSON decode failure", dlq_rec.error_reason)

            await dlq_sub.ack(dlq_msg)
            await bus.shutdown()


if __name__ == "__main__":
    unittest.main()
