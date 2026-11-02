# apister: Compression and Storage Optimization Guide

This guide provides deep technical documentation on `apister`'s compression subsystems, end-to-end zero-recompression pipelines, filesystem-level storage optimizations, and resilience patterns.

---

## 1. Architectural Philosophy: Zero-Recompression

In traditional distributed messaging architectures, data undergoes multiple costly compression and decompression cycles across its lifecycle:

```
[Producer]                   [Broker / Storage]               [Consumer]
 Compress (Zstd) ──Network─► Decompress ──Disk Write (Snappy) ──Disk Read ──Decompress ──Network (Gzip) ──► Decompress
```

Each cycle incurs:
1. **High CPU overhead** (decompressing and recompressing consumes significant core cycles).
2. **Memory churn** (allocating and freeing transient decompression buffers triggers garbage collection spikes).
3. **Increased latency** (serialization and transcoding serialization stalls).

### The `apister` Zero-Recompression Invariant

`apister` treats compressed frames as immutable, opaque binary payloads:

```
[Ingest Stream]                                            [apister Engine]                                   [Consumer Pool]
.tar.zst / .parquet / .json.zst ──Stream Chunk (64KB)──► Native Append-Only Ledger (Zero-Copy) ──MMAP Read──► Zero-Copy Consumer Slice
                                                                                                           (Decompress on Demand)
```

1. **Direct Ingestion**: Compressed files, archives, and network streams are spooled directly into the append-only ledger without decompression.
2. **Transparent Chunking**: Large compressed streams are split into bounded chunks that preserve framing boundaries without decoding payload bytes.
3. **Zero-Copy Memory Mapping**: Consumers receive memory-mapped slices (`memoryview`) directly from OS page caches, decompressing only in the terminal consumer worker threads.
4. **Direct Broker Forwarding**: Bridged brokers (Kafka, Redis, RabbitMQ) receive the pre-compressed byte buffer verbatim, avoiding egress re-compression.

---

## 2. Supported Compression Codecs

`apister` provides pluggable compression codecs backed by SIMD-accelerated Rust kernels (`crates/apister_core`) with seamless pure Python fallbacks:

| Codec | Python Class | Native Rust Acceleration | Typical Compression Ratio | Compression Speed | Decompression Speed | Best Suited For |
|---|---|---|---|---|---|---|
| **Zstandard (Zstd)** | `ZstdCodec` | Yes (`zstd-rs`) | **3.5x – 6.0x** | 250 MB/s (L3) | **1.8 GB/s** | Default high-efficiency data at rest & transport |
| **Zstd + Dictionary** | `ZstdCodec(dict_data)` | Yes (`zdict_builder`) | **4.0x – 8.5x** | 320 MB/s (L3) | **2.2 GB/s** | Micro-messages (< 1 KB), JSON/Avro sensor telemetry |
| **LZ4** | `LZ4Codec` | Yes (`lz4_flex`) | **2.0x – 2.8x** | **780 MB/s** | **4.2 GB/s** | Ultra-low latency, real-time market data feeds |
| **Gzip** | `GzipCodec` | Python stdlib | 2.5x – 3.8x | 65 MB/s | 350 MB/s | Legacy cross-language integration |
| **None (Passthrough)** | `NoopCodec` | Zero-overhead | 1.0x | Line Rate | Line Rate | Pre-compressed payloads (.parquet, .png, .zst) |

---

## 3. Pretrained Dictionaries for Micro-Messages

### The Small Message Compression Dilemma

Standard compression algorithms (Zstandard, Deflate, LZ4) rely on identifying repeating patterns within a sliding window (typically 32KB to 8MB). When message payloads are small (< 1KB), such as IoT telemetry or trade execution events:
- The compressor cannot build a statistical model or dictionary from 200 bytes of data.
- The compressed frame can actually be **larger** than the original payload due to framing headers.

### Solution: Statistical Dictionary Training

`apister` implements offline or runtime dictionary training. By feeding representative message samples into `train_zstd_dictionary()`, Zstandard extracts the common structural elements (JSON field names, XML tags, repeated schemas) into a compact dictionary (typically 16KB–64KB):

```python
from apister.compression import ZstdCodec, train_zstd_dictionary

# 1. Collect representative training samples from production traffic
samples = [
    b'{"device_id": "sensor_001", "voltage": 12.4, "temp_c": 21.3, "status": "nominal"}',
    b'{"device_id": "sensor_002", "voltage": 12.1, "temp_c": 22.8, "status": "nominal"}',
    # ... 500+ representative samples
]

# 2. Train a 32 KB statistical dictionary
dict_bytes = train_zstd_dictionary(samples, dict_size=32 * 1024)

# 3. Instantiate dictionary-accelerated codec
codec = ZstdCodec(level=3, dict_data=dict_bytes)

# 4. Micro-message compression: 85 bytes -> 22 bytes (3.8x reduction)
payload = b'{"device_id": "sensor_049", "voltage": 12.2, "temp_c": 21.9, "status": "nominal"}'
compressed = codec.compress(payload)
decompressed = codec.decompress(compressed)
```

In production telemetry workloads, dictionary-accelerated Zstandard achieves **3.8x to 5.2x compression ratios on sub-256-byte payloads**, cutting disk I/O and network egress costs by over 70%.

---

## 4. Storage-Level vs. Filesystem-Level Compression

Architects evaluating `apister` on Linux/Unix systems have two primary layers for compression:

```
┌────────────────────────────────────────────────────────┐
│               Application Layer (apister)              │
│   • Packaged Zstd / LZ4 Framing (\x00CMP)              │
│   • Pretrained Dictionary for Micro-Messages           │
│   • End-to-end compressed wire transit                │
└───────────────────────────┬────────────────────────────┘
                            │ Writes to Ledger
┌───────────────────────────▼────────────────────────────┐
│         Filesystem Layer (ZFS / Btrfs / XFS)           │
│   • Block-level transparent compression                │
│   • ZFS zstd-fast / lz4 recordsize=128k                │
│   • NVMe hardware offload                              │
└────────────────────────────────────────────────────────┘
```

### Comparative Analysis Matrix

| Feature | Filesystem-Level (ZFS / Btrfs) | Application-Level (`apister.compression`) |
|---|---|---|
| **Scope** | All files in directory / dataset | Selected topics or payloads |
| **CPU Utilization** | Kernel worker threads | Application worker threads |
| **Micro-Message Efficiency** | Low (operates on 64KB/128KB record blocks) | **High** (dictionary optimizes each 200B message) |
| **Transport Preservation** | None (data is uncompressed when read) | **Yes** (sent over network already compressed) |
| **Memory Footprint** | Kernel ARC cache | Application heap / memoryview |
| **Portability** | Requires ZFS / Btrfs kernel modules | Runs anywhere (Linux, macOS, Windows) |

### Recommended Production Blueprint

1. **For General Enterprise Deployments (Windows, Ext4, XFS)**:
   - Use `apister.compression.pack_compressed(payload, codec="zstd")` at the application layer.
   - For micro-telemetry (< 1KB), train and distribute a shared Zstandard dictionary.
2. **For High-Throughput Linux Ingestion on ZFS**:
   - Set dataset compression to `lz4` or `zstd-1`:
     ```bash
     zfs create -o compression=zstd-1 -o recordsize=128k pool/apister_ledger
     ```
   - Run `apister` in raw mode (`codec="none"`), allowing the kernel to handle block-level compression across 64MB segment files with zero application CPU overhead.

> [!WARNING] Double-Compression Anti-Pattern
> Never compress payloads with application-level Zstandard (Level 9+) and write them to a ZFS dataset configured with `zstd`. Pre-compressed data appears as high-entropy random bytes, causing the filesystem compressor to waste CPU cycles attempting and failing to compress the blocks.

---

## 5. Optimized Transport & Broker Wire Framing

When bridging `apister` to distributed message brokers (Kafka, Redis, RabbitMQ), payloads can be packed with a self-describing 5-byte header:

```
0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       \x00    |       'C'     |       'M'     |       'P'     |  (Magic 4 bytes)
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Codec ID    |          Compressed Binary Payload...         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

**Codec Identifiers**:
- `0x00`: Raw uncompressed
- `0x01`: Zstandard (`zstd`)
- `0x02`: LZ4 frame (`lz4`)
- `0x03`: Gzip (`gzip`)

### Transparent Auto-Unpacking Example

```python
from apister.compression import pack_compressed, unpack_compressed

# Producer: pack with Zstd
wire_payload = pack_compressed(json_bytes, codec="zstd")

# Consumer: auto-detects 0x01 and decompresses, or returns raw bytes if uncompressed
original_bytes = unpack_compressed(wire_payload)
```

---

## 6. Dead Letter Queue (DLQ) & Poison-Pill Containment

A single malformed payload ("poison pill") must never block subscriber queues or cause worker loops to crash indefinitely. `apister.dlq` provides automated retry policies and routing to dead-letter topics.

```
Incoming Message ──► Subscriber Handler ──► [Success] ──► Ack
                           │
                         [Fail]
                           │
                    Retry < Max (3)? ──► Exponential Backoff & Retry
                           │
                         [No]
                           ▼
                    Route to .dlq Topic ──► Original Cursor Acknowledged
                    (DeadLetterRecord)
```

### Implementing DLQ Protected Subscribers

```python
from apister import Bus
from apister.dlq import DeadLetterPolicy, handle_with_dlq


async def process_order(envelope):
    # Business logic that may raise ValueError on corrupted data
    validate_order(envelope.payload)


async def worker_loop(bus: Bus):
    sub = await bus.subscribe("orders/#", cursor="order-worker")
    policy = DeadLetterPolicy(
        max_retries=3,
        dlq_topic_suffix=".dlq",
        retry_delay_seconds=0.1,
    )

    async for envelope in sub:
        # Executes with automated retry and routes to "orders/us.dlq" on 3rd failure
        ok = await handle_with_dlq(bus, envelope, process_order, policy=policy)
        await sub.ack(envelope)
```

---

## 7. Distributed Tracing: W3C Trace Context Propagation

`apister` implements native W3C Trace Context (`traceparent`) injection and extraction for end-to-end distributed observability across microservices and async worker pipelines:

$$\text{traceparent} = \text{00}-\underbrace{\text{4bf92f3577b34da6a3ce929d0e0e4736}}_{\text{Trace ID (32 hex)}}-\underbrace{\text{00f067aa0ba902b7}}_{\text{Span ID (16 hex)}}-\underbrace{\text{01}}_{\text{Sampled Flag}}$$

### Zero-Allocation In-Band Tracing

```python
from apister.tracing import (
    generate_traceparent,
    inject_trace_context,
    extract_trace_context,
)

# 1. Producer: inject trace header into payload
traceparent = generate_traceparent()
payload_with_trace = inject_trace_context(b'{"order_id": 9941}', traceparent)
await bus.publish("orders/checkout", payload_with_trace)

# 2. Consumer: extract trace context
envelope = await sub.get()
tp, raw_payload = extract_trace_context(envelope.payload)
# tp -> "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
# raw_payload -> b'{"order_id": 9941}'
```

When compiled with the native Rust core (`crates/apister_core`), `rust_inject_trace` and `rust_extract_trace` slice payloads in native memory with zero Python object allocations.

---

## 8. Early Disk Space Warning & Storage Guard

`apister` continuously monitors ledger filesystem capacity during its background sync loop.

### Configuration

```python
bus = await Bus.open(
    "/var/data/apister",
    disk_warning_threshold=0.85,  # Warn when disk capacity exceeds 85%
)

stats = bus.stats()
print(f"Disk Free: {stats.disk_free_bytes / (1024**3):.2f} GB")
print(f"Disk Usage: {stats.disk_usage_percent * 100:.1f}%")
if stats.disk_warning:
    trigger_pagerduty_alert("apister storage capacity critical")
```

If filesystem usage reaches `disk_warning_threshold`, the writer loop emits a structured alert at `WARNING` severity every 5 seconds, providing ops teams with early warning before out-of-disk write failures occur.
