---
name: apister-pubsub
description: Expert guidance for designing, implementing, and optimizing durable in-process pub/sub systems using apister in asyncio Python applications. Use when writing publishers, batch publishing, subscriber worker pools, durable consumer cursors, backpressure policies, zero-copy buffer manipulation, or replaying ledger data.
---

# Apister Pub/Sub Agent Skill

## Overview

`apister` is an in-process, high-fan-out asyncio publish-subscribe system backed by an append-only, segmented on-disk ledger with an optional hybrid Rust native engine.

### Key Architectural Concepts
1. **Zero-Copy Fan-Out**: Payloads are appended **once** to disk/cache. Subscriber queues store 64-bit integer byte offsets, not copies of payloads.
2. **Hybrid Rust Core (`backend="auto"`)**: Automatically leverages native Rust acceleration (`crates/apister_core`) for **1.26M+ writes/sec**, hardware SIMD CRC32, memory-mapped reads (`mmap`), and radix topic trie matching.
3. **Batch Publishing (`publish_many`)**: Atomically writes and fans out sequences of messages in a single writer dispatch turn.
4. **Zero-Copy Buffer Protocol**: `envelope.memoryview()` and `envelope.to_numpy()` allow direct tensor and buffer access without heap allocations.
5. **LRU Tail Cache**: Active subscribers read recent payloads from memory without hitting the disk.
6. **Monotonic Ack Cursors**: Named subscriptions persist their acknowledged byte cursor on disk. Acks can occur out-of-order, but the cursor only advances monotonically past contiguous acknowledged prefixes.
7. **Flow Control & Parking**: If a subscriber's unacknowledged message count exceeds `inflight_limit`, further deliveries are parked on disk without blocking the publisher loop.

---

## 1. Opening and Initializing the Bus

Always open the bus using the async context manager or explicitly shut it down in a `finally` block:

```python
from pathlib import Path
from apister import Bus, OverflowPolicy

async with await Bus.open(
    Path("data/bus"),
    backend="auto",  # 'auto' (default, uses Rust if present), 'rust', or 'python'
    max_queue_size=1024,  # Max offset items per subscriber queue
    command_queue_size=4096,  # Command queue depth for writer loop
    overflow=OverflowPolicy.BLOCK,  # Backpressure strategy
    segment_bytes=64 * 1024 * 1024,  # Rotate segments at 64 MiB
    commit_every=128,  # Fsync every 128 appends
    fsync_interval_s=0.01,  # Periodic fsync interval (10 ms)
    retain_bytes=10 * 1024 * 1024 * 1024,  # Retention cap (10 GiB)
    tail_cache_bytes=32 * 1024 * 1024,  # 32 MiB in-memory hot tail
) as bus:
    # Run application logic
    ...
```

### Production Tuning Rules
- **Backend Selection**: Default to `backend="auto"`. Use `backend="rust"` in performance-critical deployments where the C-extension is required.
- **Memory Footprint**: `max_payload_bytes` defaults to 8 MiB. Increase if broadcasting large embeddings or media chunks.
- **Fsync Cadence**: Lower `fsync_interval_s` (e.g. 0.005) or `commit_every=1` for critical durability; keep default (10ms / 128 records) for high throughput.
- **Retention Quota**: Always set `retain_bytes` to bound disk space. When exceeded, the oldest sealed `.seg` files are dropped automatically.

---

## 2. Topic Matching & Wildcards

Topics use `/` delimiters.
- `+`: Matches exactly one segment (e.g., `devices/+/telemetry`).
- `#`: Matches zero or more trailing segments and **must be at the end** (e.g., `orders/#`).
- Exact strings match only that specific topic.

```python
# Exact match
exact_sub = await bus.subscribe("orders/eu/checkout")

# Single-level wildcard
zone_sub = await bus.subscribe("orders/+/checkout")

# Multi-level wildcard
all_orders_sub = await bus.subscribe("orders/#")
```

Topic matching features $O(1)$ match memoization (**16.1M+ matches/sec**) on hot topics, automatically cleared when subscriptions change.

---

## 3. Publishing Patterns

### Pattern A: Single Event Publishing
Accepts `bytes`, `bytearray`, or `memoryview`:

```python
await bus.publish("telemetry/metrics", b'{"cpu": 45.2}')
```

### Pattern B: Batch Publishing (`publish_many`)
Use for high-frequency streams, orderbook ticks, or chunked document embeddings:

```python
batch = [
    ("telemetry/sensor1", b'{"val": 10.1}'),
    ("telemetry/sensor2", b'{"val": 20.4}'),
    ("telemetry/sensor3", b'{"val": 15.8}'),
]

# Atomically writes and fans out all items in a single dispatch turn:
envelopes = await bus.publish_many(batch)
```

### Pattern C: Handling Backpressure & Deadlines
When `OverflowPolicy.BLOCK` is active, slow consumers can fill queues. Pass `timeout` on time-sensitive writes:

```python
from apister import PublishTimeout

try:
    await bus.publish("events/live", payload, timeout=0.5)
except PublishTimeout:
    # Downstream queues were saturated; ledger remains untouched!
    logger.warning("Dropped message due to downstream backpressure")
```

---

## 4. Consumer Implementation Patterns

### Pattern A: Ephemeral Live Stream Consumer
Use for non-critical telemetry, dashboards, and live feeds:

```python
async with await bus.subscribe("metrics/#") as sub:
    async for envelope in sub:
        process_metric(envelope.topic, envelope.payload)
```

### Pattern B: Zero-Copy NumPy & Memoryview Consumer
Use for computer vision, audio DSP, and machine learning pipelines:

```python
import numpy as np

sub = await bus.subscribe("camera/front/frames")
async for envelope in sub:
    # 1. Zero-copy buffer slice:
    buf: memoryview = envelope.memoryview()

    # 2. Direct NumPy array mapping (zero heap allocations):
    frame_np: np.ndarray = envelope.to_numpy(dtype=np.uint8)

    # Ingest directly into OpenCV / PyTorch / ONNX Runtime:
    process_tensor(frame_np)
```

### Pattern C: Durable Worker Pool with Acknowledgment Regulator
Use for reliable job queues where messages must not be lost on failure or restart:

```python
from apister import SubscriptionClosed

sub = await bus.subscribe(
    "jobs/compute",
    name="worker-alpha",  # Persistent cursor name
    inflight_limit=10,  # Maximum unacked messages at once
)

try:
    while True:
        envelope = await sub.get()
        try:
            await execute_task(envelope.payload)
            # Acknowledge task completion
            await sub.ack(envelope)
        except Exception as exc:
            logger.error("Job processing failed: %s", exc)
            # Do NOT ack. Restarting will resume from the oldest unacked prefix!
            break
except SubscriptionClosed:
    logger.info("Bus shut down gracefully")
```

### Pattern D: Replaying Historical Log
Replay from a known byte offset or from the beginning (`replay_from=0`):

```python
audit_sub = await bus.subscribe("audit/#", replay_from=0)
```

---

## 5. Flow Control & Backpressure Policies

| Policy | Behavior | Best Used For |
| :--- | :--- | :--- |
| `OverflowPolicy.BLOCK` | Halts writer loop if a subscriber queue fills until slots open or `timeout` expires. | Zero data loss, transactional messaging, worker queues. |
| `OverflowPolicy.DROP_OLDEST` | Evicts the oldest queued offset for the slow subscriber. | Real-time tickers, metrics, UI streaming where freshest state matters. |
| `OverflowPolicy.DROP_NEWEST` | Rejects the incoming offset for the slow subscriber. | Rate-capped alert streams. |

### Global vs Per-Subscription Inflight Caps
- **Per-Subscription**: `await bus.subscribe("pattern", inflight_limit=5)`
- **Global Pattern Default**: `await bus.limit_inflight("jobs/#", limit=16)`

---

## 6. Shutdown & Signal Handling

Always capture termination signals to allow clean cursor and disk commits:

```python
import asyncio
import signal
from apister import Bus


async def main() -> None:
    bus = await Bus.open("data/bus", backend="auto")

    def stop():
        asyncio.create_task(bus.shutdown())

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop)

    # Run application...
```

---

## 7. Common Pitfalls & Anti-Patterns to Avoid

1. **Missing `sub.ack()` in Durable Subscriptions**:
   If `name` is provided and messages are never acknowledged, `inflight_limit` will eventually fill, parking all future deliveries to that subscriber indefinitely.
2. **Synchronous Blocking Inside Consumer Loops**:
   Always use `await asyncio.sleep(...)` or offload blocking CPU/IO calls to `asyncio.to_thread()`. Never block the event loop inside `async for envelope in sub`.
3. **Omitting `retain_bytes` in Production**:
   Always set `retain_bytes` so old segments are automatically rotated and cleaned up from disk.
4. **Improper Cursor Names**:
   Cursor names must match `^[A-Za-z0-9._-]{1,64}$`. Slashes and spaces are rejected.
5. **Cross-Process Expectation Without a Broker Bridge**:
   `apister` is in-process. To connect multiple Python processes across machines, use `bus.attach_broker(RedisBroker(...))` or `KafkaBroker(...)`.
