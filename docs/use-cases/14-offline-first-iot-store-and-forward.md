# Guide 14: Offline-First IoT Store-and-Forward & Cloud Outbox Bridge

## Problem & Architectural Challenge

Edge IoT gateways, industrial smart meters, oceanic vessels, agricultural sensors, and remote monitoring boxes operate in environments with intermittent, unstable, or satellite-only network connectivity:
- Local sensors produce telemetry (vibration, power draw, temperature, pressure) continuously at 10–100 Hz.
- The uplink to the cloud (cellular 4G/5G, LoRaWAN, satellite Starlink/Iridium) frequently drops for minutes or hours.

### The Problem with Direct Network Publishing
If an edge application attempts to publish directly to a cloud message broker (AWS IoT Core, Azure IoT Hub, central Kafka/RabbitMQ) over an open TCP socket:
- Connection drops block or drop telemetry data.
- Buffering data in RAM leads to memory exhaustion and kernel panics during prolonged network outages.
- When the network recovers, bursting thousands of buffered events simultaneously can create echo loops or duplicate records in downstream analytics databases.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 Local Industrial Sensors & PLC              │
│               (Continuous Telemetry at 50 Hz)               │
└──────────────────────────────┬──────────────────────────────┘
                               │ publish("telemetry/factory/line_1", sensor_data)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Append-only on-disk log   │
                │  - Bounded retention quotas  │
                │  - Zero RAM exhaustion       │
                └──────────────┬───────────────┘
                               │
            Network Down       │       Network Restored
            (Buffer on Disk)   │       (Stream to Cloud)
                               ▼
                ┌──────────────────────────────┐
                │    Reconnecting Broker Bridge│
                │ (Redis / Kafka / RabbitMQ)   │
                │  - Exponential backoff/jitter│
                │  - Bounded dedup log         │
                └──────────────┬───────────────┘
                               │
                               ▼
                 Central Enterprise Cloud Cluster
```

---

## Production Implementation

```python
import asyncio
import json
import time
from pathlib import Path
from apister import Bus, OverflowPolicy
from apister.brokers import RedisBroker
from apister.types import ReconnectPolicy


async def industrial_sensor_producer(bus: Bus) -> None:
    """Continuously samples local equipment sensors."""
    for i in range(100):
        telemetry = {
            "device_id": "turbine_alpha",
            "vibration_mm_s": 1.25 + (i * 0.01),
            "temp_c": 68.4,
            "timestamp": time.time(),
        }
        # Appends to local disk ledger regardless of cloud internet status!
        await bus.publish("telemetry/factory/turbines", json.dumps(telemetry).encode("utf-8"))
        await asyncio.sleep(0.02)


async def main() -> None:
    storage = Path("data/edge_outbox")
    async with await Bus.open(
        storage,
        retain_bytes=500 * 1024 * 1024,  # Hard limit: maximum 500 MB on edge flash
        overflow=OverflowPolicy.DROP_OLDEST,  # Oldest historical records pruned if flash is full
        commit_every=64,
    ) as bus:
        # Configure auto-reconnecting cloud outbox bridge:
        cloud_broker = RedisBroker("redis://127.0.0.1:6379/0")
        try:
            await bus.attach_broker(
                cloud_broker,
                patterns=["telemetry/#"],
                policy=ReconnectPolicy(
                    initial_delay_s=0.5,
                    max_delay_s=30.0,
                    multiplier=2.0,
                    jitter_ratio=0.2,
                ),
            )
        except Exception:
            print("[Offline Mode] Cloud broker unreachable. Storing telemetry safely to disk...")

        await industrial_sensor_producer(bus)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Offline-First Edge Architectures

1. **Zero-Ops Local Persistence**: No Docker or JVM daemon required on the embedded device; runs purely in-process inside your Python service.
2. **Deterministic Flash Protection**: `retain_bytes` guarantees that extended offline periods never fill the onboard flash or crash the edge computer.
3. **Resilient Reconnection with De-duplication**: The built-in bridge automatically reconnects with exponential backoff and randomized jitter, while the bounded on-disk dedup ledger prevents duplicate records upon reconnect.
