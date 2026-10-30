# apister

[![CI & Security](https://github.com/sunilksundaram/apister/actions/workflows/ci.yml/badge.svg)](https://github.com/sunilksundaram/apister/actions/workflows/ci.yml)
[![GitHub Repository](https://img.shields.io/badge/GitHub-sunilksundaram%2Fapister-181717?logo=github)](https://github.com/sunilksundaram/apister)
[![Python Version](https://img.shields.io/badge/python->=3.14-blue.svg)](https://www.python.org/)
[![Architecture](https://img.shields.io/badge/architecture-zero--copy%20fan--out-brightgreen.svg)](#architecture)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/sunilksundaram/apister/blob/main/LICENSE)

**apister** is an in-process, high-fan-out asyncio publish-subscribe bus backed by a durable, segmented on-disk ledger.

Traditional in-process event systems copy messages to each subscriber queue in memory. When broadcasting large payloads (e.g., model embeddings, audio chunks, image frames, or high-throughput telemetry) across dozens or hundreds of subscribers, memory usage explodes and event loops stall.

`apister` writes every payload **exactly once** to an append-only ledger and delivers lightweight 64-bit byte offsets to subscriber queues. Hot payloads are served directly from an in-memory tail cache, while durable subscribers track acknowledged prefixes with restart persistence, backpressure regulation, and pluggable external broker bridges (Redis, RabbitMQ, Kafka).

---

## Key Features

- **Zero-Payload-Copy Fan-Out**: Payloads are stored once in the segmented ledger. Subscriber queues store only integer byte offsets, enabling high fan-out with minimal memory overhead.
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

`apister` requires **Python 3.14+**.

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

| Attribute | Type | Description |
| :--- | :--- | :--- |
| `offset` | `int` | Starting byte offset of this message in the ledger. |
| `next_offset` | `int` | Byte offset where the next message begins. |
| `topic` | `str` | Destination topic string (e.g. `"orders/created"`). |
| `payload` | `bytes` | Message payload content. |
| `message_id` | `bytes` | 16-byte unique identifier. |
| `timestamp_ns` | `int` | Nanoseconds timestamp when written to the ledger. |
| `from_broker` | `bool` | `True` if message was ingested from an external broker bridge. |

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

## AI Agents & Harness Skills

`apister` includes pre-packaged agent skills in [`.agent/skills`](file:///.agent/skills) designed for coding agents, LLM harnesses (Claude Code, Hermes, Cursor, GitHub Copilot, custom LangChain/CrewAI/AutoGen agents), and developer pair-programming:

- **[`apister-pubsub`](.agent/skills/apister-pubsub/SKILL.md)**: Zero-copy fan-out, persistent ack cursors, inflight message parking, and backpressure policies.
- **[`apister-broker-bridging`](.agent/skills/apister-broker-bridging/SKILL.md)**: Redis, RabbitMQ, and Kafka adapters, reconnection policies, and loop-free deduplication.

Harness integrations are pre-configured in `.cursorrules` (Cursor) and `.github/copilot-instructions.md` (GitHub Copilot).

---

## License

This project is licensed under the MIT License.
