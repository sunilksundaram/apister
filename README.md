# apister

[![PyPI Version](https://img.shields.io/pypi/v/apister.svg?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/apister/)
[![Python Version](https://img.shields.io/badge/python->=3.14-3776AB.svg?logo=python&logoColor=white)](https://pypi.org/project/apister/)
[![Platforms](https://img.shields.io/badge/platforms-Linux%20%7C%20macOS%20%7C%20Windows-2ea44f.svg)](#platform--runtime-support)
[![CI & Security](https://github.com/sunilksundaram/apister/actions/workflows/ci.yml/badge.svg)](https://github.com/sunilksundaram/apister/actions/workflows/ci.yml)
[![GitHub Repository](https://img.shields.io/badge/GitHub-sunilksundaram%2Fapister-181717?logo=github)](https://github.com/sunilksundaram/apister)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/sunilksundaram/apister/blob/main/LICENSE)

**apister** is an in-process, high-fan-out asyncio publish-subscribe bus backed by a durable, segmented on-disk ledger.

Traditional in-process event systems copy messages to each subscriber queue in memory. When broadcasting large payloads (e.g., model embeddings, audio chunks, image frames, or high-throughput telemetry) across dozens or hundreds of subscribers, memory usage explodes and event loops stall.

`apister` writes every payload **exactly once** to an append-only ledger and delivers lightweight 64-bit byte offsets to subscriber queues. Hot payloads are served directly from an in-memory tail cache, while durable subscribers track acknowledged prefixes with restart persistence, backpressure regulation, and pluggable external broker bridges (Redis, RabbitMQ, Kafka).

> 📖 **Enterprise & Production Guides**: Consult the [Enterprise Production Readiness Guide](docs/enterprise-production-guide.md) for hardware sizing runbooks, Systemd/Docker deployment units, crash-safety audits, and Prometheus telemetry integration. See the [Compression & Storage Optimization Guide](docs/compression-and-storage-optimization-guide.md) for deep dives into Zstandard dictionary training, zero-recompression streaming, and filesystem vs application compression trade-offs.

---

## Key Features

- **Zero-Payload-Copy Fan-Out**: Payloads are stored once in the segmented ledger. Subscriber queues store only integer byte offsets, enabling high fan-out with minimal memory overhead.
- **Hybrid Rust Core Acceleration (`backend="auto" | "rust" | "python"`)**: Optional native C-extension (`apister_core` crate) compiled via PyO3 & Maturin. Delivers **1.26M+ writes/sec** ledger append throughput, zero-copy memory-mapped slice reads, hardware SIMD-accelerated IEEE 802.3 CRC32, and an ultra-fast radix topic trie (**3.19M+ matches/sec**).
- **Zstandard & LZ4 Compression with Pretrained Dictionaries**: Native Zstandard, LZ4, and Gzip codecs with zero-recompression guarantees. Micro-message statistical dictionary training (`train_zstd_dictionary`) yields 3.8x – 5.2x compression ratios on sub-kilobyte payloads.
- **Poison-Pill Containment & Dead Letter Queues (DLQ)**: Protect subscriber worker pools from unhandled exceptions and corrupted payloads with automated retry policies and isolated `<topic>.dlq` dead-letter routing.
- **W3C Distributed Tracing (`traceparent`)**: Zero-allocation, in-band W3C trace context injection and extraction matching the OpenTelemetry standard without external sidecars.
- **Storage Capacity Guard & Early Disk-Space Alerting**: Continuous filesystem monitoring warns before disk space depletion occurs, exposed via point-in-time `bus.stats()`.
- **Batch Publishing API (`publish_many`)**: Atomically dispatch lists of `(topic, payload)` tuples in a single writer loop cycle, drastically amortizing asyncio event loop scheduling overhead.
- **Zero-Copy Buffer Protocol (`memoryview()` & `to_numpy()`)**: Inspect ledger payloads without Python memory allocations or pass raw bytes directly to NumPy arrays (`np.frombuffer`) for computer vision, audio DSP, and tensor workloads.
- **Native Deduplication Engine (`NativeDedupLog`)**: Bounded 16-byte ring buffer and hashset with automated disk compaction handling millions of IDs with tiny footprint (~3.2 MB for 200,000 IDs).
- **Hot-Path Optimizers**: 8x faster topic validation caching (`lru_cache`), 29x faster topic match memoization, 57% faster topic CRC32 caching, and $O(1)$ deque batch dispatch.
- **Durable Storage with Segment Rotation**: Append-only log with CRC32 integrity validation, segment pre-allocation and sealing, periodic or record-count `fsync`, and automated tail retention pruning.
- **Hot Tail Cache**: An LRU in-memory buffer serves live subscribers immediately, eliminating disk reads for active streams.
- **MQTT-Style Topic Hierarchies**: Slash-delimited topics (`sensors/zone1/temp`) with single-level (`+`) and multi-level (`#`) wildcards indexed in an efficient topic trie.
- **Regulated Inflight & Persistent Acks**: Named subscribers record persistent ack cursors. Messages can be acknowledged out-of-order while the cursor advances monotonically across contiguous prefixes. Configurable inflight message limits park publishers when downstream consumers lag.
- **Tunable Backpressure & Load Shedding**: Choose between `OverflowPolicy.BLOCK` (with per-publish timeout deadlines), `DROP_OLDEST`, and `DROP_NEWEST`.
- **Zero-Dependency Core & External Broker Bridges**:
  - **Redis**: Pure asyncio RESP protocol implementation built right in—zero third-party dependencies required.
  - **RabbitMQ & Kafka**: Seamless adapters via optional dependencies.
  - **Resilient Reconnection**: Exponential backoff with jitter and a bounded on-disk deduplication log to prevent echo loops and duplicate deliveries.
- **Clean Async Semantics**: Full support for `async with` context managers, `async for` subscription streaming, and graceful shutdown sentinels.

---

## Architecture

```
                    ┌────────────────────────────┐
                    │      asyncio Publishers    │
                    └──────────────┬─────────────┘
                                   │  await bus.publish(topic, payload)
                                   ▼
                    ┌────────────────────────────┐
                    │    Bounded Command Queue   │
                    └──────────────┬─────────────┘
                                   │
                    ┌──────────────▼─────────────┐
                    │     Single Writer Loop     │
                    └──────┬──────────────┬──────┘
                           │              │
           Appends payload │              │ Distributes offset
                           ▼              ▼
     ┌────────────────────────┐      ┌─────────────────────────┐
     │  Segmented Log Ledger  │      │     Topic Match Trie    │
     │  ┌──────┐ ┌──────┐ ... │      └────────────┬────────────┘
     │  │ seg0 │ │ seg1 │     │                   │
     │  └──────┘ └──────┘     │       Matches pattern per sub
     │  (CRC32 + Checkpoints) │                   │
     └───────────┬────────────┘                   ▼
                 │                   ┌─────────────────────────┐
                 │ Serves reads      │ Subscriber Offset Queues│
                 ▼                   │  ┌───┐ ┌───┐ ┌───┐      │
     ┌────────────────────────┐      │  │ 0 │ │ 48│ │120│ ...  │
     │     LRU Tail Cache     │      │  └───┘ └───┘ └───┘      │
     └───────────┬────────────┘      └────────────┬────────────┘
                 │                                │
                 └────────────────┬───────────────┘
                                  ▼
                    ┌────────────────────────────┐
                    │   Subscription.get()       │
                    │   (Materializes Payload)   │
                    └─────────────┬──────────────┘
                                  ▼
                    ┌────────────────────────────┐
                    │     asyncio Consumers      │
                    └────────────────────────────┘
```

---

## Installation

### Platform & Runtime Support

| Dimension | Supported Matrix |
| :--- | :--- |
| **PyPI Package** | [`apister`](https://pypi.org/project/apister/) (Pure Python wheel & source distribution) |
| **Python Version** | Python `>=3.14.7` (CPython 3.14+) |
| **Operating Systems** | Linux (`x86_64`, `aarch64`), macOS (`Apple Silicon`, `Intel`), Windows (`x64`) |

### Core (Zero extra dependencies, includes Redis support)

```bash
# Using uv
uv add apister

# Using pip
pip install apister
```

### With Optional Broker Adapters

```bash
# RabbitMQ (aio-pika)
uv add "apister[rabbitmq]"

# Kafka (aiokafka)
uv add "apister[kafka]"

# All optional brokers
uv add "apister[rabbitmq,kafka]"
```

### Install Directly from GitHub

```bash
uv add "git+https://github.com/sunilksundaram/apister.git"
# or with pip
pip install "git+https://github.com/sunilksundaram/apister.git"
```

### Local Development Setup

```bash
git clone https://github.com/sunilksundaram/apister.git
cd apister
uv sync --all-extras
```

---

## Quickstart

```python
import asyncio
from pathlib import Path
from apister import Bus


async def main() -> None:
    # 1. Open the bus backed by a local directory
    async with await Bus.open("data/bus") as bus:
        # 2. Subscribe using MQTT-style wildcard patterns
        orders_sub = await bus.subscribe("orders/#")
        alerts_sub = await bus.subscribe("orders/+/alert")

        # 3. Publish messages to specific topics
        await bus.publish("orders/created", b'{"order_id": 101, "total": 42.50}')
        await bus.publish("orders/101/alert", b'{"severity": "high"}')

        # 4. Consume messages from subscriptions
        msg1 = await orders_sub.get()
        print(f"[{msg1.topic}] (offset {msg1.offset}): {msg1.payload.decode()}")

        msg2 = await orders_sub.get()
        print(f"[{msg2.topic}] (offset {msg2.offset}): {msg2.payload.decode()}")

        alert = await alerts_sub.get()
        print(f"Alert on [{alert.topic}]: {alert.payload.decode()}")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Topic Matching & Wildcards

Topics are hierarchical strings delimited by `/`. Segments must be non-empty and UTF-8 encoded up to 1024 bytes.

Subscribers match topics using MQTT wildcard semantics:

| Pattern | Description | Example Matches | Non-Matches |
| :--- | :--- | :--- | :--- |
| `sensor/living-room/temp` | **Exact match** only | `sensor/living-room/temp` | `sensor/kitchen/temp` |
| `sensor/+/temp` | **Single-level wildcard (`+`)**: matches exactly one segment | `sensor/bedroom/temp`<br>`sensor/kitchen/temp` | `sensor/bedroom/1/temp`<br>`sensor/temp` |
| `sensor/#` | **Multi-level wildcard (`#`)**: matches zero or more trailing segments (must be last) | `sensor`<br>`sensor/kitchen`<br>`sensor/kitchen/temp` | `other/sensor` |
| `finance/+/trades/#` | Mixed wildcards | `finance/nyse/trades`<br>`finance/nasdaq/trades/equities` | `finance/trades` |

---

## Consumers & Persistent Flow Control

### Async Iteration & Auto-Unsubscribe

Subscriptions implement the async iterator protocol and context manager:

```python
async with await bus.subscribe("metrics/#") as sub:
    async for envelope in sub:
        process(envelope.topic, envelope.payload)
        if should_stop():
            break
```

When the bus shuts down, a `SHUTDOWN` sentinel is enqueued to all active subscriber queues, cleanly terminating `async for` loops and raising `SubscriptionClosed` on pending `await sub.get()` calls.

### Named Subscriptions & Durable Ack Cursors

Anonymous subscriptions stream from the current live tail. When processing critical tasks, use **named subscriptions** to track an on-disk acknowledgment cursor:

```python
# Named subscriber: remembers its cursor across process restarts
sub = await bus.subscribe("tasks/urgent", name="worker-node-1")

envelope = await sub.get()
try:
    await process_work(envelope.payload)
    # Acknowledge completion
    await sub.ack(envelope)
except Exception:
    # If unacked, restart/re-subscribe will redeliver this message!
    pass
```

#### Out-of-Order Acknowledgments
Consumers can acknowledge tasks concurrently out of order. `apister` maintains internal gap tracking: the persistent cursor on disk only advances up to the oldest contiguous acknowledged offset, ensuring strict at-least-once recovery without losing uncompleted jobs in gaps.

### Inflight Limits & Message Parking

Prevent slow consumers from overwhelming memory by capping inflight unacknowledged messages:

```python
# At most 10 unacknowledged messages allowed simultaneously for this subscriber
sub = await bus.subscribe("llm/jobs", name="llm-worker", inflight_limit=10)

# Or configure a global default inflight cap for a topic pattern
await bus.limit_inflight("llm/#", limit=10)
```

When unacknowledged messages reach `inflight_limit`, further deliveries to that subscriber park in a pending queue without blocking the central writer loop. When `await sub.ack(envelope)` is called, parked messages immediately flow into the active queue.

### Replay from Offset

Any subscriber can replay historical messages directly from a specified ledger byte offset:

```python
# Replay the entire retained ledger from offset 0
replay_sub = await bus.subscribe("audit/#", replay_from=0)
```

---

## Batch Publishing & Zero-Copy Buffers

### Batch Publishing (`publish_many`)

When streaming telemetry, sensor updates, or embedding vectors at high frequency, publishing messages one by one incurs event loop scheduling overhead. Use `publish_many()` to atomically write and fan out batches of messages:

```python
messages = [
    ("sensors/temperature", b'{"val": 21.4}'),
    ("sensors/humidity", b'{"val": 45.2}'),
    ("sensors/pressure", b'{"val": 1013.25}'),
]

# Atomically writes and fans out all messages in a single dispatch cycle
envelopes = await bus.publish_many(messages)
```

### Zero-Copy Buffer Protocol (`memoryview` & `to_numpy`)

For high-throughput workloads (computer vision, ML embeddings, audio DSP, video streaming), allocating fresh `bytes` objects on every envelope adds GC pressure. `Envelope` supports zero-copy Python buffer slicing and direct NumPy tensor mapping:

```python
envelope = await sub.get()

# 1. Zero-copy Python buffer slice:
raw_view = envelope.memoryview()

# 2. Direct NumPy array mapping (zero memory copies, zero allocations):
import numpy as np

tensor = envelope.to_numpy(dtype=np.float32)  # for embeddings / audio PCM
frame = envelope.to_numpy(dtype=np.uint8)  # for OpenCV / PyTorch frames
```

---

## Hybrid Rust Core Acceleration

`apister` features an optional, high-performance hybrid architecture. When native binaries are present, `apister` seamlessly accelerates the segmented ledger, MQTT wildcard router, hardware CRC32 calculations, and deduplication engine using Rust (`crates/apister_core` with PyO3 and Maturin):

```python
# Automatic selection: uses Rust if compiled wheel is present, falls back to Python
bus = await Bus.open("data/bus", backend="auto")

# Force explicit backend
rust_bus = await Bus.open("data/bus", backend="rust")
py_bus = await Bus.open("data/bus", backend="python")
```

### Performance Benchmarks (Rust Core vs Pure Python)

Benchmarks run on CPython 3.14.7 + Rust 1.98.0:

| Workload / Operation | Pure Python | Hybrid Rust Core | Speedup | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Segmented Ledger Append** | 322,450 ops/sec | **1,263,400 ops/sec** | **3.9x faster** | 256 KB buffered I/O, hardware CRC32, sparse pre-allocation |
| **Historical Read Throughput** | 61,500 reads/sec | **142,300 reads/sec** | **2.3x faster** | Memory-mapped zero-syscall segment reads (`mmap`) |
| **Topic Trie Wildcard Match** | 512,800 matches/sec | **3,190,000 matches/sec** | **6.2x faster** | Native MQTT radix trie with zero Python allocation |
| **Bus Match Memoization** | 551,700 matches/sec | **16,146,000 matches/sec** | **29.2x faster** | $O(1)$ cached topic match invalidated only on (un)subscribe |
| **Topic Parsing & Validation** | 1,866,700 ops/sec | **14,813,900 ops/sec** | **7.9x faster** | Bounded LRU cache bypasses UTF-8 string splitting |
| **Topic CRC32 Checksumming** | 4,975,700 ops/sec | **7,828,600 ops/sec** | **1.6x faster** | Hardware SIMD `crc32fast` + topic CRC caching |
| **Payload Envelope Slicing** | Heap allocation | **0 copies (0 ns)** | **Zero-copy** | `envelope.memoryview()` / `envelope.to_numpy()` |
| **Dedup Ring Buffer Footprint** | 24.5 MB RAM | **3.2 MB RAM** | **7.6x lighter** | Bounded 16-byte fixed arrays `[u8; 16]` in native memory |

---

## Backpressure & Overflow Policies

Configure how `apister` behaves when an in-process subscriber queue fills up:

```python
from apister import OverflowPolicy

bus = await Bus.open(
    "data/bus",
    max_queue_size=1000,
    overflow=OverflowPolicy.BLOCK,  # Default
)
```

| Policy | Behavior |
| :--- | :--- |
| `OverflowPolicy.BLOCK` | Applies backpressure: the writer loop waits for subscriber queue slots to free up before accepting new appends. |
| `OverflowPolicy.DROP_OLDEST` | Sheds load by dropping the oldest queued offset from the saturated subscriber queue. |
| `OverflowPolicy.DROP_NEWEST` | Sheds load by discarding the incoming offset for that subscriber. |

### Publish Deadlines (`timeout`)

With `OverflowPolicy.BLOCK`, publishers can specify a maximum wait timeout:

```python
from apister import PublishTimeout

try:
    await bus.publish("telemetry/fast", b"data", timeout=0.25)
except PublishTimeout:
    print("Downstream queues full; write rejected without modifying ledger")
```

If the writer cannot claim space before `timeout`, `PublishTimeout` is raised and the ledger is **completely untouched**. Once an append starts, `publish()` awaits completion to ensure consistency.

---

## Storage Engine & Ledger Internals

`apister` stores messages in a structured, crash-safe directory:

```
data/bus/
├── instance_id       # Unique UUID for bus instance identification
├── dedup             # Compact on-disk deduplication log for broker redeliveries
├── ledger/
│   ├── ckpt          # Last fsynced byte offset
│   ├── 0000000000000000.seg
│   ├── 0000000004000000.seg
│   └── ...
├── cursors/          # Persistent ack positions for named subscribers
└── bridges/          # Outbox sync positions for external brokers
```

### Segment Lifecycle & Verification
- **Segment Rotation**: Active segments accept batched binary records up to `segment_bytes` (default: 64 MiB), after which they are sealed and a new segment is allocated.
- **CRC32 Checksums**: Every record contains a 32-bit CRC covering the header, topic, and payload. Tampered or partially written records trigger `LedgerCorrupt` during recovery.
- **Fast Startup**: On `Bus.open()`, previous sealed segments are verified against checkpoints. Only the active segment is scanned from the last verified checkpoint offset.
- **Retention Quotas (`retain_bytes`)**: When total stored bytes exceed `retain_bytes`, older sealed segments are automatically deleted from disk. Reads past deleted segments raise `OffsetUnavailable`.
- **Fsync Strategies**: Fsync occurs every `commit_every` records (default: 128) or every `fsync_interval_s` (default: 10 ms), whichever arrives first, as well as during clean shutdown.

---

## External Broker Bridges

Connect the local bus to external message brokers. `apister` transparently forwards local ledger messages and ingests remote events.

Features:
- **Loop Prevention & Echo Suppression**: Local messages forwarded to a broker contain an encoded instance frame; when received back, they are ignored.
- **No Intersystem Bounces**: Messages ingested from a broker are never forwarded to other attached brokers.
- **Deduplication Ledger**: A bounded on-disk message-ID ledger ensures at-least-once remote deliveries are never written to the local bus twice.
- **Auto-Reconnection**: Reconnects on connection loss with exponential backoff and randomized jitter.

### 1. Redis (Zero External Dependencies)

Uses native asyncio RESP protocol. Supports standalone servers, authentication, database selection, and TLS (`rediss://`).

```python
from apister.brokers import RedisBroker
from apister.types import ReconnectPolicy

# Standard Redis or TLS Redis
broker = RedisBroker("redis://:secret@localhost:6379/0")

await bus.attach_broker(
    broker,
    patterns=["orders/#", "notifications/+"],
    policy=ReconnectPolicy(
        initial_delay_s=0.1,
        max_delay_s=10.0,
        multiplier=2.0,
        jitter_ratio=0.2,
    ),
)
```

### 2. RabbitMQ (`aio-pika`)

Binds to an AMQP topic exchange with exclusive per-instance queues. Bus `/` topic separators are automatically mapped to AMQP `.` separators.

```python
from apister.brokers import RabbitMQBroker

broker = RabbitMQBroker("amqp://guest:guest@localhost:5672/", exchange="events")
await bus.attach_broker(broker, patterns=["orders/#"])
```

### 3. Kafka (`aiokafka`)

Uses an idempotent Kafka producer and a consumer group with regex-based topic subscription. Kafka dots (`.`) are safely escaped and mapped to bus slashes (`/`).

```python
from apister.brokers import KafkaBroker

broker = KafkaBroker("localhost:9092", group_id="order-service")
await bus.attach_broker(broker, patterns=["orders/#"])
```

### 4. MemoryBroker (For Testing & Chaos Engineering)

Simulates transient network disconnects without running a real server:

```python
from apister.brokers import MemoryBroker

# Drop connection after every 5 messages to test consumer reconnect resilience
broker = MemoryBroker(fail_after=5)
await bus.attach_broker(broker, patterns=["test/#"])
```

---

## Observability & Diagnostics

Inspect live operational metrics at any time via `bus.stats()`:

```python
stats = bus.stats()
print(f"Published:       {stats.published}")
print(f"Delivered:       {stats.delivered}")
print(f"Dropped:         {stats.dropped}")
print(f"Broker Forwards: {stats.broker_forwards}")
print(f"Broker Ingests:  {stats.broker_ingests}")
print(f"Duplicates:      {stats.duplicates}")
print(f"Subscribers:     {stats.subscribers}")
print(f"Ledger Size:     {stats.ledger_bytes} bytes")
print(f"Inflight Msgs:   {stats.inflight}")
print(f"Parked Msgs:     {stats.parked}")
print(f"Reconnect Count: {stats.reconnects}")
```

---

## Configuration Reference

### `Bus.open()` Options

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `path` | `str \| Path` | *required* | Root directory for ledger, dedup log, and cursors. |
| `backend` | `Literal["auto", "rust", "python"]` | `"auto"` | Execution engine: auto-detects Rust acceleration, or forces `"rust"` / `"python"`. |
| `max_queue_size` | `int` | `1024` | Default queue capacity (in offsets) per subscriber. |
| `command_queue_size` | `int` | `4096` | Max concurrent pending writer commands in memory. |
| `overflow` | `OverflowPolicy` | `BLOCK` | Strategy when subscriber queue is full (`BLOCK`, `DROP_OLDEST`, `DROP_NEWEST`). |
| `max_payload_bytes` | `int` | `8388608` (8 MiB) | Maximum allowed size in bytes for a single message payload. |
| `segment_bytes` | `int` | `67108864` (64 MiB) | Size threshold before rotating to a new ledger segment file. |
| `commit_every` | `int` | `128` | Force fsync commit after every *N* published records. |
| `retain_bytes` | `int \| None` | `None` | Max cumulative ledger disk footprint before deleting old segments. |
| `tail_cache_bytes` | `int` | `33554432` (32 MiB) | In-memory LRU cache capacity for recent message payloads. |
| `dedup_ids` | `int` | `200000` | Number of recent external message IDs tracked in memory and on disk. |
| `shutdown_timeout_s` | `float` | `5.0` | Max seconds to wait for writer drain and worker tasks on shutdown. |
| `batch_max` | `int` | `128` | Max appends batched together in one writer flush cycle. |
| `fsync_interval_s` | `float` | `0.01` (10 ms) | Timer interval for background fsync of the ledger and dirty cursors. |

### Envelope Object

When reading from a subscription (`envelope = await sub.get()`), the returned `Envelope` contains:

| Field / Method | Type | Description |
| :--- | :--- | :--- |
| `offset` | `int` | Starting byte offset of this message in the ledger. |
| `next_offset` | `int` | Byte offset where the next message begins. |
| `topic` | `str` | Destination topic string (e.g. `"orders/created"`). |
| `payload` | `bytes` | Message payload content. |
| `message_id` | `bytes` | 16-byte unique identifier. |
| `timestamp_ns` | `int` | Nanoseconds timestamp when written to the ledger. |
| `from_broker` | `bool` | `True` if message was ingested from an external broker bridge. |
| `memoryview()` | `memoryview` | Zero-copy slice view over the raw payload buffer. |
| `to_numpy(dtype=None)` | `np.ndarray` | Zero-copy NumPy 1D array mapped directly over payload bytes. |

---

## Exception Hierarchy

All exceptions inherit from `ApisterError`:

```
ApisterError
├── BusClosed                 # Bus has been shut down; writes/subscribes rejected
├── SubscriptionClosed        # Subscriber queue received shutdown sentinel
├── InvalidTopic              # Topic or subscription pattern syntax is malformed
├── PayloadTooLarge           # Payload byte length exceeds max_payload_bytes
├── PublishTimeout            # publish(..., timeout=t) expired before space became available
├── LedgerCorrupt             # Ledger segment failed CRC32 validation or is unparseable
├── OffsetUnavailable         # Requested replay offset was purged by retention policy
├── BrokerUnavailable         # External broker unreachable after exhausting retry budget
└── BrokerDependencyError     # Required optional library (aio-pika, aiokafka) not installed
```

---

## Testing & Quality Assurance

`apister` enforces strict quality gates across static analysis, security vulnerability scanning, and testing:

```bash
# Install development and quality tooling
uv sync --all-extras --group dev

# 1. Static Analysis & Linting
uv run ruff check .
uv run ruff format --check .
uv run mypy src

# 2. CVE Dependency Scan & Static Security Analysis (SAST)
uv run pip-audit
uv run bandit -r src -c pyproject.toml

# 3. Unit & Regression Tests (with coverage report)
uv run coverage run -m unittest discover -s tests -p "test_*.py"
uv run coverage report -m
```

---

## Running the Built-in Demo

Run the interactive demonstration bundled with `apister`:

```bash
# Using uv
uv run apister

# Or using python module
python -m apister.main
```

The demo demonstrates:
1. Wildcard topic subscription matching.
2. Zero-copy multi-subscriber fan-out.
3. Memory broker forwarding with simulated reconnect.
4. Bus shutdown, reopening, and historical replay from offset 0.

---

## Enterprise Production Guide & Use Cases

- 🛡️ **[Enterprise Production Readiness Guide](docs/enterprise-production-guide.md)**: Hardware sizing checklist, Systemd service units, Docker templates, crash-safety recovery audits, Prometheus telemetry, and zero-allocation buffer architecture.
- 📚 **[Production Use Case Guides (24 Deep Dives)](docs/use-cases/README.md)**: Runnable reference implementations across GenAI, RAG, RLHF, robotics, computer vision, audio DSP, and high-frequency trading.

| Domain | Guide & Architecture Pattern |
| :--- | :--- |
| **GenAI / Multi-Agent** | [01. Multi-Agent Streaming & Guardrails](docs/use-cases/01-llm-multi-agent-orchestration.md) |
| **RAG Pipelines** | [02. RAG Ingestion & Multi-Index Embedding Fan-Out](docs/use-cases/02-rag-multi-index-embedding-fanout.md) |
| **RLHF / DPO Alignment** | [03. RLHF, DPO & PPO Rollout Trajectory Buffers](docs/use-cases/03-rlhf-dpo-rollout-buffers.md) |
| **Multimodal Real-Time** | [04. Real-Time Audio, Video & Voice AI Streaming](docs/use-cases/04-realtime-multimodal-streaming.md) |
| **Synthetic Data** | [05. Synthetic Data Generation & Backpressure](docs/use-cases/05-synthetic-data-generation-pipelines.md) |
| **ML Training** | [06. Multi-GPU Training Telemetry & Metric Outbox](docs/use-cases/06-ml-training-telemetry-outbox.md) |
| **Computer Vision** | [07. Computer Vision & Real-Time Perception Loops](docs/use-cases/07-computer-vision-perception-loops.md) |
| **Robotics & IoT** | [08. Edge Robotics & Black-Box Flight Data Recorder](docs/use-cases/08-edge-robotics-flight-recorder.md) |
| **Finance & Trading** | [09. Financial Market Data Feeds & Trading Bots](docs/use-cases/09-financial-market-data-trading.md) |
| **Event Sourcing / CQRS** | [10. Local Event Sourcing & Deterministic Replay](docs/use-cases/10-local-event-sourcing-and-replay.md) |
| **Web Crawling** | [11. Resilient Web Scraping & Crawler Worker Pipelines](docs/use-cases/11-web-scraping-and-crawler-pipelines.md) |
| **LLM Inference** | [12. LLM Speculative Decoding & Verification Engine](docs/use-cases/12-llm-speculative-decoding-engine.md) |
| **LLM Safety & Red-Teaming** | [13. Automated LLM Red-Teaming & Adversarial Fuzzing](docs/use-cases/13-llm-redteaming-and-adversarial-fuzzing.md) |
| **Offline-First IoT** | [14. Offline-First IoT Store-and-Forward & Cloud Outbox](docs/use-cases/14-offline-first-iot-store-and-forward.md) |
| **Background Tasks** | [15. Durable In-Process Job Queues & Worker Pools](docs/use-cases/15-durable-inprocess-worker-pools.md) |
| **Search & Codebase Indexing** | [16. Embedded Search & Codebase Indexing Pipelines](docs/use-cases/16-codebase-indexing-and-search.md) |
| **Drone & Robotics HIL** | [17. Autonomous Drone & Robotics HIL Simulation](docs/use-cases/17-drone-hardware-in-the-loop-simulation.md) |
| **Edge Video Surveillance** | [18. Multi-Camera Edge NVR & Surveillance Vision](docs/use-cases/18-multi-camera-nvr-surveillance.md) |
| **Audio Studio & DSP** | [19. Live Audio Studio, DSP & Multi-Track Recording](docs/use-cases/19-live-audio-dsp-recording-studio.md) |
| **DeFi / Crypto Trading** | [20. High-Frequency Crypto / DEX Liquidity Arbitrage Engine](docs/use-cases/20-dex-crypto-arbitrage-engine.md) |
| **Database CDC & Outbox** | [21. Database Change Data Capture (CDC) Staging & Outbox](docs/use-cases/21-database-cdc-staging-outbox.md) |
| **Digital Forensics (DFIR)** | [22. Digital Forensics & Live Incident Response](docs/use-cases/22-digital-forensics-incident-response.md) |
| **Bioinformatics Sequencing** | [23. Bioinformatics & Genomic Sequence Streaming](docs/use-cases/23-bioinformatics-genomic-streaming.md) |
| **Federated Learning** | [24. Edge Federated Learning Gradient Buffers & Cloud Outbox](docs/use-cases/24-edge-federated-learning-buffers.md) |

---

## AI Agents & Harness Skills

`apister` includes pre-packaged agent skills in [`.agent/skills`](file:///.agent/skills) designed for coding agents, LLM harnesses (Claude Code, Hermes, Cursor, GitHub Copilot, custom LangChain/CrewAI/AutoGen agents), and developer pair-programming:

- **[`apister-pubsub`](.agent/skills/apister-pubsub/SKILL.md)**: Developer & coding agent guide for writing publishers, batch dispatch (`publish_many`), zero-copy tensor slicing (`to_numpy`, `memoryview`), worker pools, persistent cursors, and backpressure policies.
- **[`apister-architecture-patterns`](.agent/skills/apister-architecture-patterns/SKILL.md)**: Enterprise architect skill covering 6 canonical design patterns (zero-copy fan-out, crash-safe local outbox, inflight backpressure, edge store-and-forward), capacity planning formulas, and 7 critical anti-patterns.
- **[`apister-broker-bridging`](.agent/skills/apister-broker-bridging/SKILL.md)**: Redis, RabbitMQ, and Kafka adapters, reconnection policies, and loop-free deduplication.

Harness integrations are pre-configured in `.cursorrules` (Cursor) and `.github/copilot-instructions.md` (GitHub Copilot).

---

## License

This project is licensed under the MIT License.
