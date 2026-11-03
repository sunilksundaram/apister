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
from apister import Bus, OverflowPolicy, ZstdCodec, train_zstd_dictionary
from apister.brokers import RedisBroker
from apister.types import ReconnectPolicy


async def industrial_sensor_producer(bus: Bus, codec: ZstdCodec) -> None:
    """Continuously samples local equipment sensors and appends dictionary-compressed records."""
    for i in range(100):
        telemetry = {
            "device_id": "turbine_alpha",
            "vibration_mm_s": 1.25 + (i * 0.01),
            "temp_c": 68.4,
            "timestamp": time.time(),
        }
        # Compress with pretrained dictionary: ~4.5x compression ratio on sub-KB IoT telemetry
        raw_json = json.dumps(telemetry).encode("utf-8")
        compressed_bytes = codec.compress(raw_json)

        # Appends to local disk ledger regardless of cloud internet status!
        await bus.publish("telemetry/factory/turbines", compressed_bytes)
        await asyncio.sleep(0.02)


async def main() -> None:
    storage = Path("data/edge_outbox")

    # Train or load a compact 32 KiB Zstandard dictionary on typical sensor payloads
    sample_records = [
        json.dumps(
            {
                "device_id": "turbine_alpha",
                "vibration_mm_s": 1.25,
                "temp_c": 68.0,
                "timestamp": 1700000000.0,
            }
        ).encode("utf-8")
        for _ in range(50)
    ]
    dict_bytes = train_zstd_dictionary(sample_records, dict_size=32 * 1024)
    codec = ZstdCodec(level=3, dict_data=dict_bytes)

    async with await Bus.open(
        storage,
        retain_bytes=500 * 1024 * 1024,  # Hard limit: maximum 500 MB on edge flash
        overflow=OverflowPolicy.DROP_OLDEST,  # Oldest historical records pruned if flash is full
        commit_every=64,
        disk_warning_threshold=0.85,  # Alert before edge SD card/eMMC storage is exhausted
    ) as bus:
        # Configure auto-reconnecting cloud outbox bridge:
        # Zero-recompression invariant: compressed payloads stream straight to cloud brokers
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

        await industrial_sensor_producer(bus, codec)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Offline-First Edge Architectures

1. **Zero-Ops Local Persistence**: No Docker or JVM daemon required on the embedded device; runs purely in-process inside your Python service.
2. **Deterministic Flash Protection**: `retain_bytes` guarantees that extended offline periods never fill the onboard flash or crash the edge computer.
3. **Resilient Reconnection with De-duplication**: The built-in bridge automatically reconnects with exponential backoff and randomized jitter, while the bounded on-disk dedup ledger prevents duplicate records upon reconnect.
