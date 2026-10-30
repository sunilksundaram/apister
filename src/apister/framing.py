"""Wire framing for broker bridges.

External payloads are wrapped so a bus can ignore its own echo and deduplicate
redelivery. Unframed bytes are accepted as foreign messages.
"""

from __future__ import annotations

_VERSION = 1
_ID_LEN = 16


def encode_frame(instance_id: bytes, message_id: bytes, payload: bytes) -> bytes:
    if len(instance_id) != _ID_LEN or len(message_id) != _ID_LEN:
        raise ValueError("instance_id and message_id must be 16 bytes")
    return bytes((_VERSION,)) + instance_id + message_id + payload


def decode_frame(data: bytes) -> tuple[bytes, bytes, bytes] | None:
    """Return ``(instance_id, message_id, payload)`` or ``None`` if unframed."""
    if len(data) < 1 + _ID_LEN + _ID_LEN or data[0] != _VERSION:
        return None
    start = 1
    instance_id = data[start : start + _ID_LEN]
    mid_at = start + _ID_LEN
    message_id = data[mid_at : mid_at + _ID_LEN]
    return instance_id, message_id, data[mid_at + _ID_LEN :]
