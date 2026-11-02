"""Wire framing for broker bridges.

External payloads are wrapped so a bus can ignore its own echo and deduplicate
redelivery. Unframed bytes are accepted as foreign messages.
"""

from __future__ import annotations

import zlib

_VERSION = 1
_ID_LEN = 16
_MAGIC = b"AP"


def encode_frame(instance_id: bytes, message_id: bytes, payload: bytes) -> bytes:
    if len(instance_id) != _ID_LEN or len(message_id) != _ID_LEN:
        raise ValueError("instance_id and message_id must be 16 bytes")
    header_ids = instance_id + message_id
    crc = (zlib.crc32(header_ids) & 0xFFFFFFFF).to_bytes(4, "big")
    return _MAGIC + bytes((_VERSION,)) + header_ids + crc + payload


def decode_frame(data: bytes) -> tuple[bytes, bytes, bytes] | None:
    """Return ``(instance_id, message_id, payload)`` or ``None`` if unframed."""
    # V2 format with magic and header CRC32 check
    min_v2 = len(_MAGIC) + 1 + _ID_LEN + _ID_LEN + 4
    if len(data) >= min_v2 and data.startswith(_MAGIC) and data[len(_MAGIC)] == _VERSION:
        start = len(_MAGIC) + 1
        instance_id = data[start : start + _ID_LEN]
        mid_at = start + _ID_LEN
        message_id = data[mid_at : mid_at + _ID_LEN]
        crc_at = mid_at + _ID_LEN
        expected_crc = int.from_bytes(data[crc_at : crc_at + 4], "big")
        actual_crc = zlib.crc32(instance_id + message_id) & 0xFFFFFFFF
        if expected_crc == actual_crc:
            return instance_id, message_id, data[crc_at + 4 :]

    # Legacy V1 format compatibility fallback
    if len(data) >= 1 + _ID_LEN + _ID_LEN and data[0] == _VERSION:
        start = 1
        instance_id = data[start : start + _ID_LEN]
        mid_at = start + _ID_LEN
        message_id = data[mid_at : mid_at + _ID_LEN]
        return instance_id, message_id, data[mid_at + _ID_LEN :]

    return None
