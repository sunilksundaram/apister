"""Pluggable high-performance compression codecs for data at rest and in transit.

Supports:
- Zstandard (with native Rust accelerator and pre-trained dictionaries)
- LZ4 (sub-millisecond streaming frame compression)
- Gzip (standard library fallback)
- No-op (zero-overhead identity passthrough)
- Pretrained dictionary training for micro-messages
"""

from __future__ import annotations

import gzip
from typing import Protocol, runtime_checkable

from apister.native import (
    HAS_RUST_CORE,
    native_lz4_compress,
    native_lz4_decompress,
    native_zstd_compress,
    native_zstd_decompress,
    native_zstd_train_dict,
)

# Optional Python library fallbacks
try:
    import zstandard as zstd  # type: ignore[import-untyped,import-not-found]
except ImportError:
    zstd = None

try:
    import lz4.frame as lz4_frame  # type: ignore[import-untyped,import-not-found]
except ImportError:
    lz4_frame = None


@runtime_checkable
class CompressionCodec(Protocol):
    """Protocol for synchronous message payload compression and decompression."""

    @property
    def name(self) -> str:
        """Name of the compression algorithm."""
        ...

    def compress(self, data: bytes) -> bytes:
        """Compress raw byte sequence into compressed frame."""
        ...

    def decompress(self, data: bytes) -> bytes:
        """Decompress compressed frame back into original byte sequence."""
        ...


class NoopCodec:
    """Zero-overhead passthrough codec."""

    name = "none"

    def compress(self, data: bytes) -> bytes:
        return data

    def decompress(self, data: bytes) -> bytes:
        return data


class ZstdCodec:
    """Zstandard codec with optional dictionary acceleration and native Rust execution."""

    name = "zstd"

    def __init__(
        self,
        level: int = 3,
        dict_data: bytes | None = None,
    ) -> None:
        self.level = level
        self.dict_data = dict_data
        self._cctx = None
        self._dctx = None

        if not HAS_RUST_CORE:
            if zstd is None:
                raise ImportError(
                    "Zstandard is not available. Install 'apister[compression]' or compile the Rust core."
                )
            zdict = zstd.ZstdCompressionDict(dict_data) if dict_data else None
            self._cctx = zstd.ZstdCompressor(level=level, dict_data=zdict)
            self._dctx = zstd.ZstdDecompressor(dict_data=zdict)

    def compress(self, data: bytes) -> bytes:
        if HAS_RUST_CORE:
            return native_zstd_compress(data, self.level, self.dict_data)
        if self._cctx is None:
            raise RuntimeError("Zstandard compressor context is not initialized")
        return self._cctx.compress(data)

    def decompress(self, data: bytes) -> bytes:
        if HAS_RUST_CORE:
            return native_zstd_decompress(data, self.dict_data)
        if self._dctx is None:
            raise RuntimeError("Zstandard decompressor context is not initialized")
        return self._dctx.decompress(data)


class LZ4Codec:
    """LZ4 frame codec optimized for ultra-low latency compression."""

    name = "lz4"

    def compress(self, data: bytes) -> bytes:
        if HAS_RUST_CORE:
            return native_lz4_compress(data)
        if lz4_frame is None:
            raise ImportError(
                "LZ4 is not available. Install 'apister[compression]' or compile the Rust core."
            )
        return lz4_frame.compress(data)

    def decompress(self, data: bytes) -> bytes:
        if HAS_RUST_CORE:
            return native_lz4_decompress(data)
        if lz4_frame is None:
            raise ImportError(
                "LZ4 is not available. Install 'apister[compression]' or compile the Rust core."
            )
        return lz4_frame.decompress(data)


class GzipCodec:
    """Gzip compression using Python's standard library."""

    name = "gzip"

    def __init__(self, compresslevel: int = 6) -> None:
        self.compresslevel = compresslevel

    def compress(self, data: bytes) -> bytes:
        return gzip.compress(data, compresslevel=self.compresslevel)

    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)


def train_zstd_dictionary(
    samples: list[bytes],
    dict_size: int = 65536,
) -> bytes:
    """Train a Zstandard dictionary from representative message samples.

    Uses native Rust training if available, falling back to python-zstandard.
    """
    if not samples:
        raise ValueError("samples cannot be empty")

    if HAS_RUST_CORE:
        return native_zstd_train_dict(samples, dict_size)

    if zstd is None:
        raise ImportError(
            "Zstandard dictionary training requires 'apister[compression]' or the compiled Rust core."
        )

    dict_obj = zstd.train_dictionary(dict_size, samples)
    return dict_obj.as_bytes()


# Wire framing constants for automatic compressed envelope decoding
_CODEC_RAW = 0x00
_CODEC_ZSTD = 0x01
_CODEC_LZ4 = 0x02
_CODEC_GZIP = 0x03
_COMPRESSION_MAGIC = b"\x00CMP"


def pack_compressed(
    payload: bytes,
    codec: CompressionCodec | str = "zstd",
    *,
    dict_data: bytes | None = None,
) -> bytes:
    """Compress payload and package it with a self-describing 5-byte header.

    Header layout:
      `\\x00CMP` (4 bytes magic) + codec_id (1 byte: 0x01=zstd, 0x02=lz4, 0x03=gzip).
    """
    codec_instance: CompressionCodec
    codec_id: int

    if isinstance(codec, str):
        c_lower = codec.lower()
        if c_lower in ("zstd", "zstandard"):
            codec_instance = ZstdCodec(dict_data=dict_data)
            codec_id = _CODEC_ZSTD
        elif c_lower == "lz4":
            codec_instance = LZ4Codec()
            codec_id = _CODEC_LZ4
        elif c_lower == "gzip":
            codec_instance = GzipCodec()
            codec_id = _CODEC_GZIP
        elif c_lower in ("none", "noop", "raw"):
            return payload
        else:
            raise ValueError(f"Unsupported compression codec name: {codec}")
    else:
        codec_instance = codec
        if codec_instance.name == "zstd":
            codec_id = _CODEC_ZSTD
        elif codec_instance.name == "lz4":
            codec_id = _CODEC_LZ4
        elif codec_instance.name == "gzip":
            codec_id = _CODEC_GZIP
        else:
            return codec_instance.compress(payload)

    compressed_body = codec_instance.compress(payload)
    return _COMPRESSION_MAGIC + bytes([codec_id]) + compressed_body


def unpack_compressed(
    payload: bytes,
    dict_data: bytes | None = None,
) -> bytes:
    """Inspect payload prefix; decompress if packaged with compression header."""
    if len(payload) >= 5 and payload[:4] == _COMPRESSION_MAGIC:
        codec_id = payload[4]
        data = payload[5:]
        if codec_id == _CODEC_ZSTD:
            return ZstdCodec(dict_data=dict_data).decompress(data)
        if codec_id == _CODEC_LZ4:
            return LZ4Codec().decompress(data)
        if codec_id == _CODEC_GZIP:
            return GzipCodec().decompress(data)
        raise ValueError(f"Unknown compression codec id: {codec_id}")
    return payload


__all__ = [
    "CompressionCodec",
    "GzipCodec",
    "LZ4Codec",
    "NoopCodec",
    "ZstdCodec",
    "pack_compressed",
    "train_zstd_dictionary",
    "unpack_compressed",
]
