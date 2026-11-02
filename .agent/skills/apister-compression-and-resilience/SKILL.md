---
name: apister-compression-and-resilience
description: Guidance for implementing high-performance compression, pretrained dictionary training, zero-recompression streaming, dead-letter queues (DLQ), distributed W3C tracing, and disk capacity guarding in apister.
---

# apister Compression and Resilience Skill

Use this skill when designing or implementing compression pipelines, micro-payload dictionary training, poison-pill containment (DLQ), distributed tracing context propagation, and storage resilience in `apister`.

---

## 1. Zero-Recompression Ingest and Processing

### Concept
`apister` preserves pre-compressed frames (e.g. `.parquet`, `.tar.zst`, `.json.zst`) without decompressing upon ingestion or write. Payloads are written to disk as opaque binary buffers and forwarded to brokers verbatim.

### Guidelines
1. **Never decompress before publishing**: If data arrives compressed from an external source or upstream edge, publish it directly into `apister`.
2. **Terminal worker decompression**: Let downstream subscribers decompress in their own worker threads, isolating CPU decompression overhead from the central bus loop.
3. **Use Zero-Copy Buffer Protocol**: Call `envelope.memoryview()` or `envelope.to_numpy()` to read payload buffers without intermediate string or bytes copies.

---

## 2. Pretrained Dictionaries for Micro-Messages

### When to Use
Use Zstandard dictionary compression whenever individual messages are smaller than 1 KB (e.g., IoT telemetry, status updates, financial order events) and share a recurring schema or JSON keys.

### Usage Pattern
```python
from apister.compression import ZstdCodec, train_zstd_dictionary

# 1. Train dictionary using representative sample messages
samples = [
    b'{"sensor": "temp_01", "reading": 23.4, "unit": "c", "status": "ok"}',
    b'{"sensor": "temp_02", "reading": 24.1, "unit": "c", "status": "ok"}',
    # ... at least 50+ representative samples
]
dict_bytes = train_zstd_dictionary(samples, dict_size=32 * 1024)

# 2. Configure codec with dictionary
codec = ZstdCodec(level=3, dict_data=dict_bytes)

# 3. Compress / Decompress with 3x-5x improved compression ratio
compressed = codec.compress(sample_payload)
original = codec.decompress(compressed)
```

---

## 3. Wire Framing for Transport Compression

Use `pack_compressed()` and `unpack_compressed()` for self-describing wire framing across external message brokers (Kafka, Redis, RabbitMQ):

```python
from apister.compression import pack_compressed, unpack_compressed

# Wire packaging adds a 5-byte header: \x00CMP + codec_id (0x01: zstd, 0x02: lz4, 0x03: gzip)
packed = pack_compressed(raw_bytes, codec="zstd")

# Transparent unpacking auto-detects compression header or returns raw bytes
restored = unpack_compressed(packed)
```

---

## 4. Dead Letter Queue (DLQ) & Poison-Pill Containment

Prevent unparseable or corrupted messages from causing subscriber worker loops to crash indefinitely or block processing pipelines.

```python
from apister import Bus
from apister.dlq import DeadLetterPolicy, handle_with_dlq


async def process_envelope(envelope):
    data = json.loads(envelope.payload)
    ...


async def subscriber_worker(bus: Bus):
    sub = await bus.subscribe("telemetry/#", cursor="telemetry-worker")
    policy = DeadLetterPolicy(
        max_retries=3,
        dlq_topic_suffix=".dlq",
        retry_delay_seconds=0.05,
    )

    async for envelope in sub:
        # Automatically retries up to 3 times before routing to "telemetry/device_01.dlq"
        success = await handle_with_dlq(bus, envelope, process_envelope, policy=policy)
        await sub.ack(envelope)
```

---

## 5. Distributed Tracing: W3C Trace Context

Propagate W3C `traceparent` headers in-band across `apister` topics without external metadata sidecars:

```python
from apister.tracing import (
    generate_traceparent,
    inject_trace_context,
    extract_trace_context,
)

# Producer: inject traceparent header
traceparent = (
    generate_traceparent()
)  # e.g., '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'
payload = inject_trace_context(b'{"order_id": 1234}', traceparent)
await bus.publish("orders/new", payload)

# Consumer: extract trace context
envelope = await sub.get()
trace_id, clean_payload = extract_trace_context(envelope.payload)
```

---

## 6. Storage Capacity Guarding

Monitor disk capacity using `bus.stats()` and set early warning thresholds:

```python
bus = await Bus.open(
    "/var/data/apister",
    disk_warning_threshold=0.90,  # Alert at 90% capacity
)

stats = bus.stats()
if stats.disk_warning:
    # Trigger alerting or throttle high-volume ingest
    logger.critical(
        "apister storage threshold exceeded: %.1f%% used", stats.disk_usage_percent * 100
    )
```
