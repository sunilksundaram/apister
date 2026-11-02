# apister: Installation, Configuration & Parameter Tuning Guide

This guide provides exhaustive operational documentation for installing, configuring, and performance-tuning **`apister`**. It details the intuitive defaults configured out of the box, explores every configuration parameter, and provides production-ready configuration profiles tailored for specific workload characteristics.

---

## 1. Installation Options

`apister` targets Python 3.14+ and provides a modular architecture: a lightweight, zero-dependency pure Python core with optional high-performance native acceleration and broker bridging.

### A. Pure Python Installation (Zero Dependencies)

The core engine has **zero external runtime dependencies** (even the built-in Redis broker bridge is implemented in pure asyncio RESP protocol):

```bash
# Using pip
pip install apister

# Using uv
uv add apister
```

### B. High-Performance Compression Extra

To enable native Zstandard, statistical dictionary training, and streaming LZ4 frame compression:

```bash
pip install "apister[compression]"
# or: uv add "apister[compression]"
```

### C. External Message Broker Extras

```bash
# RabbitMQ bridge (aio-pika)
pip install "apister[rabbitmq]"

# Apache Kafka bridge (aiokafka)
pip install "apister[kafka]"

# All optional extras
pip install "apister[all]"
```

### D. Native Rust Core Accelerator (`crates/apister_core`)

For extreme performance (**1.26M+ writes/sec** ledger append throughput, hardware SIMD-accelerated CRC32, and an ultra-fast radix topic trie delivering **3.19M+ matches/sec**), install or compile the native Rust core:

```bash
# Clone and build locally using maturin
git clone https://github.com/sunilksundaram/apister.git
cd apister
uv run maturin develop --manifest-path crates/apister_core/Cargo.toml --release
```

**Verifying Accelerator Status in Python**:
```python
from apister import HAS_RUST_CORE

if HAS_RUST_CORE:
    print("🚀 apister running with SIMD Rust Core accelerator (_apister_rs)")
else:
    print("🐍 apister running in pure Python mode")
```

---

## 2. Intuitive & Smart Defaults

`apister` is designed to be **production-ready with zero manual configuration**. When you start the bus with only a storage path:

```python
import asyncio
from apister import Bus


async def main():
    async with await Bus.open("./data/bus") as bus:
        await bus.publish("sensors/temperature", b"23.5C")
```

The bus automatically activates carefully tuned default settings:

| Parameter | Smart Default | Rationale & Operational Intent |
|---|---|---|
| `backend` | `"auto"` | Transparently selects the compiled Rust SIMD core if present; automatically falls back to pure Python without configuration. |
| `max_queue_size` | `1024` offsets | Bounded per-subscriber queue size. Because queues store 8-byte integer offsets rather than copies, 1,024 items occupy only ~8 KB RAM. |
| `command_queue_size` | `4096` commands | Bounded admission queue for concurrent publishers before the single writer event loop. |
| `overflow` | `OverflowPolicy.BLOCK` | Zero message loss by default. Fast publishers are throttled rather than dropping messages silently. |
| `max_payload_bytes` | `8 MiB` (8,388,608 bytes) | Rejects accidental multi-gigabyte payloads to protect process memory. |
| `segment_bytes` | `64 MiB` (67,108,864 bytes) | Optimal segment size balancing file descriptor usage, mmap page cache efficiency, and disk defragmentation. |
| `commit_every` | `128` records | Batches fsync operations across writes to protect NVMe/SSD endurance while preserving high throughput. |
| `fsync_interval_s` | `0.01` (10 ms) | Durability barrier: at most 10 milliseconds of committed data is at risk in an abrupt hardware power failure. |
| `tail_cache_bytes` | `32 MiB` (33,554,432 bytes) | High-hit-rate LRU cache that serves live subscribers directly from memory, eliminating disk I/O for real-time consumers. |
| `dedup_ids` | `200_000` message IDs | Retains a bounded ring buffer of recent IDs (~3.2 MB RAM) to prevent duplicate redelivery and bridge echo loops. |
| `retain_bytes` | `None` (Unlimited) | Retains all historical segments until explicit operational retention policies are defined. |
| `disk_warning_threshold` | `0.95` (95% full) | Emits structured alerts when underlying filesystem capacity exceeds 95% full. |
| `batch_max` | `128` records | Maximum number of publish commands drained in a single writer loop cycle to prevent publisher starvation. |
| `shutdown_timeout_s` | `5.0` seconds | Grace period for consumers to drain their queues and write persistent cursors during process termination. |

---

## 3. Comprehensive Configuration Parameter Reference

Every parameter of `Bus.open()` controls a specific architectural trade-off between **throughput**, **latency**, **memory consumption**, and **hardware durability**.

### 1. `path: str | Path`
- **Description**: The filesystem directory where the segmented ledger, cursor states, deduplication ring-buffers, and instance metadata are stored.
- **Directory Layout Created**:
  - `ledger/`: Holds sealed segments (`00000000000000000000.seg`) and active append logs.
  - `cursors/`: Atomic cursor state files tracking monotonic acknowledged offsets for named subscribers.
  - `dedup/`: Bounded on-disk deduplication journal.
  - `bridges/`: External broker bridge checkpoint states.
- **Hardware Recommendations**:
  - **Best**: Local NVMe SSD formatted with XFS or Ext4 (or ZFS with `recordsize=128k`).
  - **Acceptable**: SATA SSD or cloud block storage (AWS EBS gp3, GCP Hyperdisk Balanced).
  - **Not Recommended**: Network filesystems (NFS, SMB/CIFS) due to POSIX file locking and `mmap` cache coherence latency.
  - **In-Memory Volatile**: Mount a `tmpfs` RAM disk (e.g., `/dev/shm/apister`) for ultra-low latency scenarios where disk persistence across machine reboots is not required.

### 2. `backend: Literal["auto", "rust", "python"]`
- **Default**: `"auto"`
- **Options**:
  - `"auto"`: Dynamically detects `_apister_rs`. Uses the native Rust core if compiled; falls back to pure Python if absent.
  - `"rust"`: Explicitly requires the native Rust core. Raises `RuntimeError` at startup if the compiled library is missing.
  - `"python"`: Forces the pure Python engine regardless of whether the native extension is installed. Useful for debugging and testing pure Python environments.
- **Performance Impact**:
  - `"rust"`: **~1.26M+ writes/sec**, SIMD CRC32, zero-copy memory-mapped reads.
  - `"python"`: **~90,000 writes/sec**, Python `zlib.crc32`, standard file handle seeking.

### 3. `max_queue_size: int`
- **Default**: `1024`
- **Impact**: Governs the size of each subscriber's in-memory asyncio queue. Because `apister` queues store 64-bit integer byte offsets rather than payload buffers, 1,024 queued messages require only ~8 KB of RAM.
- **Tuning Advice**:
  - Increase to `8192` or `16384` for high-throughput bursts with fast consumers.
  - Keep small (`256` to `512`) if consumers process large individual payloads and you want tight memory bounds.

### 4. `command_queue_size: int`
- **Default**: `4096`
- **Impact**: The buffer capacity for publishers waiting for the single writer loop to commit data to disk.
- **Tuning Advice**:
  - If multiple concurrent async publishers frequently encounter `asyncio.QueueFull`, scale this to `16384` or `32768`.

### 5. `overflow: OverflowPolicy`
- **Default**: `OverflowPolicy.BLOCK`
- **Options**:
  - `OverflowPolicy.BLOCK`: Applies backpressure to publishers when subscriber queues fill up. Publishers can set `timeout=0.25` on `publish()` to avoid blocking indefinitely.
  - `OverflowPolicy.DROP_OLDEST`: Evicts the oldest unread offset from the subscriber queue to make room for fresh data. Ideal for real-time video/vision frames and sensor telemetry where stale data is useless.
  - `OverflowPolicy.DROP_NEWEST`: Drops the newly arrived offset while preserving queued historical messages.

### 6. `max_payload_bytes: int`
- **Default**: `8 * 1024 * 1024` (8 MiB)
- **Impact**: Maximum allowable size for a single message payload. Any attempt to publish a payload exceeding this limit raises `PayloadTooLarge` immediately, protecting the bus from runaway memory allocations.
- **Tuning Advice**:
  - For computer vision, multi-megabyte audio files, or parquet chunks, scale up to `64 * 1024 * 1024` (64 MiB).

### 7. `segment_bytes: int`
- **Default**: `64 * 1024 * 1024` (64 MiB)
- **Impact**: Defines the file size threshold at which the active ledger segment is sealed and a new segment is allocated.
- **Tuning Advice**:
  - **High-throughput environments (> 100 MB/s)**: Set to `256 * 1024 * 1024` (256 MiB) or `512 * 1024 * 1024` (512 MiB) to reduce filesystem directory inode operations.
  - **Embedded / Low-RAM devices**: Set to `16 * 1024 * 1024` (16 MiB) for smaller memory-mapped file allocations.

### 8. `commit_every: int`
- **Default**: `128` records
- **Impact**: Forces an `fsync()` system call whenever `commit_every` messages have been appended to the ledger.
- **Tuning Advice**:
  - For financial audit ledgers where every transaction must be immediately committed to disk, set `commit_every=1`.
  - For maximum throughput where periodic fsync is sufficient, set `commit_every=1024`.

### 9. `fsync_interval_s: float`
- **Default**: `0.01` (10 milliseconds)
- **Impact**: Background timer interval for committing unwritten data and cursor snapshots to persistent disk storage.
- **Tuning Advice**:
  - `0.001` (1 ms): Near-real-time persistence for mission-critical trading nodes.
  - `0.05` (50 ms) – `0.10` (100 ms): Reduces disk write amplification and extends SSD lifespan under continuous write pressure.

### 10. `tail_cache_bytes: int`
- **Default**: `32 * 1024 * 1024` (32 MiB)
- **Impact**: Size of the in-memory LRU cache storing the most recently published messages. When subscribers read messages near the live head of the stream, payloads are materialized directly from this cache with **0 disk I/O**.
- **Tuning Advice**:
  - Increase to `128 MiB` or `256 MiB` if you have dozens of concurrent subscribers reading live streams.
  - Set to `0` to disable the cache entirely in low-memory container environments.

### 11. `dedup_ids: int`
- **Default**: `200_000` IDs
- **Impact**: Maximum number of unique 16-byte message IDs tracked in the deduplication ring-buffer.
- **Memory Footprint**: ~3.2 MB for 200,000 IDs.
- **Tuning Advice**:
  - If bridging to high-volume external Kafka clusters where rebalances cause large message replays, increase to `1_000_000` (~16 MB RAM).

### 12. `retain_bytes: int | None`
- **Default**: `None` (Unlimited retention)
- **Impact**: When the total byte size of all ledger segments exceeds `retain_bytes`, the oldest sealed segment files are deleted automatically.
- **Tuning Advice**:
  - In production, always configure `retain_bytes` (e.g., `50 * 1024 * 1024 * 1024` for 50 GB) to prevent filling the host disk.

### 13. `disk_warning_threshold: float`
- **Default**: `0.95` (95% full)
- **Impact**: Filesystem utilization fraction (0.0 to 1.0) that triggers early warnings. If disk usage exceeds this threshold, the sync loop emits structured alerts at `WARNING` severity and sets `bus.stats().disk_warning = True`.

### 14. `batch_max: int`
- **Default**: `128`
- **Impact**: The maximum number of publish commands drained in a single writer loop cycle. Coalescing writes reduces CPU context-switching overhead and maximizes sequential disk I/O throughput.

### 15. `shutdown_timeout_s: float`
- **Default**: `5.0` seconds
- **Impact**: Maximum time `bus.shutdown()` waits for active subscriptions to consume remaining queued messages and flush persistent cursors before closing the writer loop.

---

## 4. Subscriber Configuration & Flow Control

Subscribers are created via `await bus.subscribe(pattern, ...)`:

```python
sub = await bus.subscribe(
    "orders/#",
    cursor="order-worker-01",  # Named persistent cursor (or alias: name="...")
    queue_size=2048,  # Per-subscriber queue capacity
    inflight_limit=32,  # Max unacked messages before parking
    replay_from=0,  # Replay from beginning (or leave None to resume)
)
```

### Parameter Details:
- **`pattern: str`**: Slash-delimited topic filter. Supports MQTT wildcards:
  - `+`: Matches exactly one topic level (`sensors/+/temperature`).
  - `#`: Matches any number of trailing topic levels (`orders/#`).
- **`cursor: str` (alias: `name`)**: Persists the acknowledgment cursor on disk under `cursors/<name>.cursor`. When the service restarts, the subscriber automatically resumes from its last contiguous acknowledged offset.
- **`inflight_limit: int | None`**: Prevents slow consumers from ballooning memory. If 32 messages are currently unacknowledged, subsequent messages for this subscriber park safely on disk without stalling publishers.
- **`replay_from: int | None`**: Starts consumption from an explicit historical offset in the ledger instead of the live tail.

---

## 5. Production Tuning Recipes (Workload Profiles)

### Profile A: Ultra-High Throughput & Low Latency (Market Data & HFT)
*Optimized for sub-millisecond dispatch and millions of messages per second.*

```python
bus = await Bus.open(
    "/dev/shm/apister_market_data",  # RAM disk (tmpfs)
    backend="rust",  # Enforce Rust SIMD core
    command_queue_size=32768,  # Large publisher admission queue
    max_queue_size=8192,  # High subscriber burst capacity
    segment_bytes=256 * 1024 * 1024,  # 256 MiB segments
    commit_every=1024,  # Batch fsync heavily
    fsync_interval_s=0.05,  # 50 ms durability window
    tail_cache_bytes=128 * 1024 * 1024,  # 128 MiB hot cache
    batch_max=512,  # Large write batch coalescing
)
```

### Profile B: Edge & IoT Embedded Systems (Raspberry Pi / Industrial Edge)
*Optimized for minimal RAM usage (< 25 MB) and flash memory (eMMC/SD) protection.*

```python
bus = await Bus.open(
    "/var/data/apister",
    backend="auto",
    command_queue_size=1024,
    max_queue_size=256,
    max_payload_bytes=1 * 1024 * 1024,  # 1 MiB max payload
    segment_bytes=16 * 1024 * 1024,  # Small 16 MiB segments
    commit_every=256,  # Protect SD card from write amplification
    fsync_interval_s=0.10,  # 100 ms fsync
    tail_cache_bytes=8 * 1024 * 1024,  # 8 MiB tail cache
    retain_bytes=2 * 1024 * 1024 * 1024,  # Hard 2 GB disk quota
    dedup_ids=50_000,  # Minimal dedup footprint (< 1 MB RAM)
)
```

### Profile C: Large Payload & Multimedia Streaming (Computer Vision & Audio DSP)
*Optimized for high-bandwidth zero-copy buffers without garbage collection spikes.*

```python
bus = await Bus.open(
    "/mnt/fast_nvme/vision_stream",
    max_payload_bytes=64 * 1024 * 1024,  # 64 MiB payload limit (4K camera frames)
    segment_bytes=512 * 1024 * 1024,  # 512 MiB segment files
    tail_cache_bytes=256 * 1024 * 1024,  # 256 MiB cache to keep video frames hot in RAM
    overflow=OverflowPolicy.DROP_OLDEST,  # Real-time: drop stale frames if worker lags
)
```

### Profile D: Maximum Durability & Financial Auditing (Zero Data Loss)
*Optimized for strict crash durability and immediate on-disk persistence.*

```python
bus = await Bus.open(
    "/var/audit/apister_ledger",
    commit_every=1,  # Fsync every single transaction
    fsync_interval_s=0.001,  # 1 millisecond sync barrier
    overflow=OverflowPolicy.BLOCK,  # Strict backpressure: never drop messages
    retain_bytes=None,  # Retain all records permanently
    disk_warning_threshold=0.85,  # Alert early at 85% capacity
)
```

---

## 6. End-to-End Usage Examples

### 1. Zero-Copy Batch Publishing & Array Processing

```python
import numpy as np
from apister import Bus


async def producer(bus: Bus):
    # Atomic batch publishing amortizes asyncio scheduling overhead
    events = [
        ("tensors/frame_1", np.random.randn(128, 128).astype(np.float32).tobytes()),
        ("tensors/frame_2", np.random.randn(128, 128).astype(np.float32).tobytes()),
    ]
    await bus.publish_many(events)


async def consumer(bus: Bus):
    sub = await bus.subscribe("tensors/#", cursor="tensor-consumer")
    async for envelope in sub:
        # Zero-copy conversion directly into NumPy array without memory allocation
        array = envelope.to_numpy(dtype=np.float32).reshape(128, 128)
        # ... process array ...
        await sub.ack(envelope)
```

### 2. Micro-Message Compression with Pre-trained Dictionaries

```python
from apister.compression import ZstdCodec, pack_compressed, train_zstd_dictionary, unpack_compressed

# 1. Train statistical dictionary from representative samples
samples = [b'{"device": "sensor_01", "temp": 22.4, "status": "ok"}' for _ in range(100)]
dict_bytes = train_zstd_dictionary(samples, dict_size=16 * 1024)

# 2. Pack self-describing wire format
payload = b'{"device": "sensor_01", "temp": 23.1, "status": "ok"}'
compressed_wire_bytes = pack_compressed(payload, codec="zstd", dict_data=dict_bytes)

# 3. Transparent unpacking
restored = unpack_compressed(compressed_wire_bytes, dict_data=dict_bytes)
assert restored == payload
```

### 3. Poison-Pill Containment with Dead Letter Queues (DLQ)

```python
from apister.dlq import DeadLetterPolicy, handle_with_dlq


async def worker(bus: Bus):
    sub = await bus.subscribe("orders/#", cursor="order-processor")
    policy = DeadLetterPolicy(max_retries=3, dlq_topic_suffix=".dlq", retry_delay_seconds=0.1)

    async def handle_order(envelope):
        # Faulty handler that may raise ValueError on corrupted payloads
        decode_order(envelope.payload)

    async for envelope in sub:
        # Automatically retries 3 times before routing to "orders/checkout.dlq"
        await handle_with_dlq(bus, envelope, handle_order, policy=policy)
        await sub.ack(envelope)
```

### 4. Distributed Tracing (W3C `traceparent`)

```python
from apister.tracing import extract_trace_context, generate_traceparent, inject_trace_context

# Publisher: inject traceparent header
traceparent = generate_traceparent()
payload = inject_trace_context(b'{"order_id": 9942}', traceparent)
await bus.publish("orders/checkout", payload)

# Subscriber: extract trace context
envelope = await sub.get()
trace_header, clean_payload = extract_trace_context(envelope.payload)
print(f"Trace Context: {trace_header}")
```
