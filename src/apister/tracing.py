"""Distributed tracing and W3C trace context propagation.

Supports zero-copy and low-overhead in-band trace context injection and
extraction matching the W3C Trace Context specification (`traceparent`).
When the compiled Rust accelerator (`_apister_rs`) is available, parsing and
injection are executed natively in Rust.
"""

from __future__ import annotations

import re
import secrets
from typing import TYPE_CHECKING, Any

from apister.native import HAS_RUST_CORE, native_extract_trace, native_inject_trace

if TYPE_CHECKING:
    from apister.types import Envelope

_TRACE_MAGIC = b"\x00TRC"
_TRACEPARENT_LEN = 55
_W3C_REGEX = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")


def generate_traceparent(
    trace_id: str | None = None,
    parent_id: str | None = None,
    sampled: bool = True,
) -> str:
    """Generate a W3C traceparent header string."""
    t_id = trace_id or secrets.token_hex(16)
    p_id = parent_id or secrets.token_hex(8)
    flags = "01" if sampled else "00"
    return f"00-{t_id.lower()}-{p_id.lower()}-{flags}"


def parse_traceparent(traceparent: str) -> dict[str, Any] | None:
    """Parse a W3C traceparent string into its components.

    Returns None if the string does not match the W3C traceparent spec.
    """
    match = _W3C_REGEX.match(traceparent.strip().lower())
    if not match:
        return None
    version, trace_id, parent_id, flags = match.groups()
    if version == "ff" or trace_id == "0" * 32 or parent_id == "0" * 16:
        return None
    return {
        "version": version,
        "trace_id": trace_id,
        "parent_id": parent_id,
        "sampled": (int(flags, 16) & 0x01) == 0x01,
        "raw_flags": flags,
    }


def inject_trace_context(
    payload: bytes,
    traceparent: str | None = None,
) -> bytes:
    """Prefix a message payload with an in-band W3C trace context.

    If traceparent is None, a new traceparent with a unique trace_id and span_id
    is generated.
    """
    if HAS_RUST_CORE:
        return native_inject_trace(payload, traceparent)

    tp = traceparent or generate_traceparent()
    if len(tp) != _TRACEPARENT_LEN:
        raise ValueError(f"traceparent must be {_TRACEPARENT_LEN} characters, got {len(tp)}")
    return _TRACE_MAGIC + tp.encode("ascii") + payload


def extract_trace_context(payload: bytes) -> tuple[str | None, bytes]:
    """Extract W3C traceparent from payload prefix if present.

    Returns a tuple of (traceparent, inner_payload). If no trace header
    is present, returns (None, original_payload).
    """
    if HAS_RUST_CORE:
        return native_extract_trace(payload)

    min_len = 4 + _TRACEPARENT_LEN
    if len(payload) >= min_len and payload[:4] == _TRACE_MAGIC:
        try:
            tp = payload[4:min_len].decode("ascii")
            return tp, payload[min_len:]
        except UnicodeDecodeError:
            return None, payload
    return None, payload


def extract_trace_from_envelope(envelope: Envelope) -> tuple[str | None, bytes]:
    """Extract W3C traceparent and unwrapped payload from an Envelope."""
    return extract_trace_context(envelope.payload)


__all__ = [
    "extract_trace_context",
    "extract_trace_from_envelope",
    "generate_traceparent",
    "inject_trace_context",
    "parse_traceparent",
]
