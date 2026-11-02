# Guide 17: Autonomous Drone & Robotics Hardware-in-the-Loop (HIL) Simulation

## Problem & Architectural Challenge

Testing autonomous drones, rovers, and robotic platforms in the field produces high-rate, multi-modal sensor streams:
- 500 Hz IMU telemetry (accelerometer, gyroscope, orientation quaternions)
- 50 Hz RTK GPS coordinates and velocity vectors
- 20 Hz LiDAR range scans and obstacle proximity data
- 100 Hz motor ESC feedback (RPM, voltage, temperatures)

When a flight anomaly occurs, robotics software engineers must perform **Hardware-in-the-Loop (HIL) Simulation**: replaying the exact recorded sensor logs into physical flight controller hardware (e.g., PX4, ArduPilot, or ROS 2 nodes) to reproduce the failure deterministically without risking physical crashes.

### The Pitfalls
- **Uncontrolled Data Loss**: High-speed sensor loops crash if the disk writer stalls the flight control coroutines.
- **Inability to Rewind or Time-Travel**: Traditional append-only text logs require complex custom parsing to seek back and forth during simulated test runs.

### The Solution: Fast Sequential Recording with Offset-Based Time Travel
`apister`’s high-throughput ring ledger records telemetry streams at microsecond latency. During simulation testing, researchers can replay the exact run starting from byte offset `0` or jump to specific anomalies.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│             Sensors & Flight Telemetry Coroutine            │
│         (IMU 500Hz, GPS 50Hz, LiDAR 20Hz, Motors 100Hz)     │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish(topic, raw_binary)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - High-throughput ledger    │
                │  - Tail-cache for live telemetry
                │  - Time-travel replayable    │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┴─────────────────────┐
         ▼                                           ▼
┌──────────────────────────────┐            ┌──────────────────────────────┐
│ Phase 1: Live Flight Mission │            │ Phase 2: HIL Sim Replay      │
│  - Black-box disk recording  │            │  - Reset offset to 0         │
│  - Ground station telemetry  │            │  - Feed virtual autopilot    │
└──────────────────────────────┘            └──────────────────────────────┘
```

---

## Production Implementation

```python
import asyncio
import struct
import time
from pathlib import Path
from apister import Bus, OverflowPolicy


def pack_imu(timestamp: float, ax: float, ay: float, az: float) -> bytes:
    """Packs 64-bit float timestamp + 3x 32-bit float accelerometer readings."""
    return struct.pack("!dfff", timestamp, ax, ay, az)


def unpack_imu(data: bytes) -> tuple[float, float, float, float]:
    return struct.unpack("!dfff", data)


async def imu_sensor_producer(bus: Bus) -> None:
    """Simulates high-rate IMU recording during a test flight."""
    start_time = time.time()
    for step in range(50):
        now = start_time + (step * 0.002)
        payload = pack_imu(now, 0.01 * step, 9.81, -0.05 * step)
        await bus.publish("telemetry/imu/accel", payload)
        await asyncio.sleep(0.005)


async def live_ground_station_monitor(bus: Bus) -> None:
    """Live monitor consumes telemetry in real time during flight."""
    sub = await bus.subscribe("telemetry/imu/+", name="ground_station")
    count = 0
    async for envelope in sub:
        ts, ax, ay, az = unpack_imu(envelope.payload)
        count += 1
        await sub.ack(envelope)
        if count >= 50:
            break


async def replay_flight_simulation(bus: Bus) -> None:
    """Post-flight HIL simulator: replays flight from byte offset 0."""
    print("\n--- Starting Hardware-in-the-Loop Simulation Replay from Offset 0 ---")
    sub = await bus.subscribe("telemetry/imu/accel")  # Ephemeral: defaults to offset 0

    replayed_records = 0
    async for envelope in sub:
        ts, ax, ay, az = unpack_imu(envelope.payload)
        replayed_records += 1
        if replayed_records % 10 == 0:
            print(
                f"  [Sim Autopilot] Step {replayed_records:02d} | T={ts:.3f} | Accel=({ax:.2f}, {ay:.2f}, {az:.2f})"
            )
        if replayed_records >= 50:
            break
    print(f"Successfully replayed all {replayed_records} flight steps deterministically.\n")


async def main() -> None:
    storage = Path("data/drone_telemetry")
    async with await Bus.open(
        storage,
        segment_bytes=8 * 1024 * 1024,
        retain_bytes=200 * 1024 * 1024,
        overflow=OverflowPolicy.DROP_OLDEST,
        commit_every=32,
    ) as bus:
        # Step 1: Live flight execution
        await asyncio.gather(
            imu_sensor_producer(bus),
            live_ground_station_monitor(bus),
        )

        # Step 2: HIL simulation replay over recorded ledger
        await replay_flight_simulation(bus)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Best Practices & Tuning

1. **Binary Packing**: Use `struct.pack()` or Protobuf/FlatBuffers rather than JSON. Compact binary payloads maximize throughput on edge flash storage.
2. **`commit_every` Group Commits**: Set `commit_every=32` or `64` to batch filesystem fsync calls, drastically reducing I/O wait times during 500 Hz telemetry logging.
3. **Deterministic Verification**: Testing new flight firmware against the exact byte offsets from a previous incident ensures regression tests are 100% reproducible.
