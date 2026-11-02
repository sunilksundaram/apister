"""Lightweight durable pub/sub for in-process fan-out and external brokers."""

from apister.brokers import KafkaBroker, MemoryBroker, RabbitMQBroker, RedisBroker
from apister.bus import Bus, Subscription
from apister.compression import (
    CompressionCodec,
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
    route_to_dlq,
)
from apister.native import HAS_RUST_CORE, NativeLedger, NativeTopicTrie, native_crc32
from apister.tracing import (
    extract_trace_context,
    extract_trace_from_envelope,
    generate_traceparent,
    inject_trace_context,
    parse_traceparent,
)
from apister.types import (
    SHUTDOWN,
    BrokerDependencyError,
    BrokerUnavailable,
    BusClosed,
    BusStats,
    Envelope,
    InvalidTopic,
    LedgerCorrupt,
    OffsetUnavailable,
    OverflowPolicy,
    PayloadTooLarge,
    PublishTimeout,
    ReconnectPolicy,
    SubscriptionClosed,
)

__all__ = [
    "SHUTDOWN",
    "BrokerDependencyError",
    "BrokerUnavailable",
    "Bus",
    "BusClosed",
    "BusStats",
    "CompressionCodec",
    "DeadLetterPolicy",
    "DeadLetterRecord",
    "Envelope",
    "GzipCodec",
    "HAS_RUST_CORE",
    "InvalidTopic",
    "KafkaBroker",
    "LZ4Codec",
    "LedgerCorrupt",
    "MemoryBroker",
    "NativeLedger",
    "NativeTopicTrie",
    "NoopCodec",
    "OffsetUnavailable",
    "OverflowPolicy",
    "PayloadTooLarge",
    "PublishTimeout",
    "RabbitMQBroker",
    "ReconnectPolicy",
    "RedisBroker",
    "Subscription",
    "SubscriptionClosed",
    "ZstdCodec",
    "extract_trace_context",
    "extract_trace_from_envelope",
    "generate_traceparent",
    "handle_with_dlq",
    "inject_trace_context",
    "native_crc32",
    "pack_compressed",
    "parse_traceparent",
    "route_to_dlq",
    "train_zstd_dictionary",
    "unpack_compressed",
]

__version__ = "0.2.0"
