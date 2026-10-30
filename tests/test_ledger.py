import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from apister.ledger import Ledger
from apister.types import LedgerCorrupt, OffsetUnavailable


class LedgerTests(unittest.TestCase):
    def test_append_survives_reopen_and_rotates(self) -> None:
        with TemporaryDirectory() as raw:
            path = Path(raw)
            ledger = Ledger(path, segment_bytes=80, commit_every=1)
            first = ledger.append("news", b"a" * 40, b"\x01" * 16, 1, from_broker=False)
            second = ledger.append("other", b"b" * 40, b"\x02" * 16, 2, from_broker=True)
            ledger.close()
            reopened = Ledger(path, segment_bytes=80, commit_every=1)
            self.assertEqual(reopened.read(first.offset).payload, b"a" * 40)
            restored = reopened.read(second.offset)
            self.assertTrue(restored.from_broker)
            self.assertEqual(restored.topic, "other")
            self.assertGreaterEqual(len(list(path.glob("*.seg"))), 2)
            reopened.close()

    def test_crc_mismatch_is_corrupt(self) -> None:
        with TemporaryDirectory() as raw:
            path = Path(raw)
            ledger = Ledger(path, commit_every=1)
            record = ledger.append("news", b"payload", b"\x03" * 16, 3, from_broker=False)
            ledger.close()
            segment = next(path.glob("*.seg"))
            data = bytearray(segment.read_bytes())
            data[record.offset + 50] ^= 0xFF
            segment.write_bytes(data)
            (path / "ckpt").unlink()
            with self.assertRaises(LedgerCorrupt):
                Ledger(path)

    def test_retention_drops_sealed_segments(self) -> None:
        with TemporaryDirectory() as raw:
            path = Path(raw)
            ledger = Ledger(path, segment_bytes=100, commit_every=1, retain_bytes=350)
            first = ledger.append("t", b"x" * 60, b"\x04" * 16, 4, from_broker=False)
            for index in range(8):
                ledger.append(
                    "t", b"y" * 60, bytes([index + 1]) + b"\x05" * 15, 5, from_broker=False
                )
            with self.assertRaises(OffsetUnavailable):
                ledger.read(first.offset)
            self.assertGreater(ledger.end_offset, first.next_offset)
            ledger.close()
