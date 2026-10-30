---
name: apister-broker-bridging
description: Best practices for connecting apister to external message brokers (Redis, RabbitMQ, Kafka) in distributed systems. Use when bridging local in-process pub/sub to external distributed queues, configuring reconnect policies, topic pattern translation, and at-least-once deduplication.
---

# Apister Broker Bridging Agent Skill

## Overview

`apister` bridges the local in-process pub/sub bus to distributed message brokers (Redis, RabbitMQ, Kafka).

### Core Invariants
1. **At-Least-Once Delivery**: External forwards and ingests guarantee at-least-once delivery.
2. **Loop Suppression**: Outbound messages are tagged with a 16-byte `instance_id`. When remote messages are ingested back, identical instance frames are detected and dropped immediately.
3. **No Cross-Broker Bridging**: Messages ingested from Broker A are stored in the local ledger for local subscribers only; they are **never** forwarded to Broker B.
4. **On-Disk Deduplication Log**: Bounded `DedupLog` records remote `message_id`s to prevent duplicate appends when remote brokers redeliver records.

---

## 1. Connecting to Redis (Zero Extra Dependencies)

`apister` includes a dependency-free, pure-asyncio Redis adapter implementing RESP protocol.

```python
from apister import Bus, ReconnectPolicy
from apister.brokers import RedisBroker

bus = await Bus.open("data/bus")

# redis://[:password@]host[:port][/db] or rediss:// for TLS
redis_broker = RedisBroker("redis://127.0.0.1:6379/0")

policy = ReconnectPolicy(
    initial_delay_s=0.05,
    max_delay_s=30.0,
    multiplier=2.0,
    jitter_ratio=0.2,
    max_attempts=None,  # Retry indefinitely
)

await bus.attach_broker(
    redis_broker,
    patterns=["orders/#", "telemetry/+"],
    policy=policy,
)
```

---

## 2. Connecting to RabbitMQ (`aio-pika`)

Requires `pip install "apister[rabbitmq]"` or `uv add "apister[rabbitmq]"`.

### Mapping & Topology
- Declares a durable AMQP `TOPIC` exchange (default: `"apister"`).
- Creates an exclusive, auto-deleted queue bound to the specified routing keys.
- Topic delimiter `/` is automatically mapped to AMQP `.` (e.g. `orders/#` -> `orders.#`).

```python
from apister.brokers import RabbitMQBroker

rabbit = RabbitMQBroker(
    "amqp://guest:guest@localhost:5672/",
    exchange="events",
)

await bus.attach_broker(rabbit, patterns=["orders/#"])
```

---

## 3. Connecting to Kafka (`aiokafka`)

Requires `pip install "apister[kafka]"` or `uv add "apister[kafka]"`.

### Mapping & Topology
- Uses `AIOKafkaProducer` with `acks="all"` and `enable_idempotence=True`.
- Uses `AIOKafkaConsumer` subscribing via compiled regular expressions.
- Topics map `/` to `.` (e.g., `orders/created` -> `orders.created`).

```python
from apister.brokers import KafkaBroker

kafka = KafkaBroker(
    bootstrap_servers="localhost:9092",
    group_id="apister-service",
)

await bus.attach_broker(kafka, patterns=["orders/#"])
```

---

## 4. Reconnection Policies

Use `ReconnectPolicy` to protect downstream brokers from retry storms:

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `initial_delay_s` | `float` | `0.05` | Delay in seconds before first reconnection attempt. |
| `max_delay_s` | `float` | `30.0` | Maximum cap on exponential backoff delay. |
| `multiplier` | `float` | `2.0` | Backoff multiplier factor. |
| `jitter_ratio` | `float` | `0.2` | Random jitter ratio (0.0 to 1.0) applied to each delay. |
| `max_attempts` | `int \| None` | `None` | Max reconnect attempts before raising `BrokerUnavailable`. `None` retries continuously. |

---

## 5. Testing with `MemoryBroker`

To test resilience or simulate broker outages in unit tests without running an external server:

```python
from apister.brokers import MemoryBroker
from apister.types import ReconnectPolicy

# Simulate a broker that drops connection after every 2 messages
broker = MemoryBroker(fail_after=2)

policy = ReconnectPolicy(initial_delay_s=0.01, max_delay_s=0.05, jitter_ratio=0)
await bus.attach_broker(broker, patterns=["events/#"], policy=policy)
```

---

## 6. Broker Metrics & Monitoring

Check the health of external bridges using `bus.stats()`:

```python
stats = bus.stats()
print(f"Broker Forwards: {stats.broker_forwards}")  # Sent to external brokers
print(f"Broker Ingests:  {stats.broker_ingests}")   # Received from external brokers
print(f"Duplicates:      {stats.duplicates}")       # Dropped by dedup log
print(f"Reconnects:      {stats.reconnects}")       # Total broker reconnect count
```

