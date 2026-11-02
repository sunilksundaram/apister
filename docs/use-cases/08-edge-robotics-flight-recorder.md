# Guide 08: Edge Robotics & Black-Box Flight Data Recorder

## Problem & Architectural Challenge

Robotics platforms (autonomous drones, delivery rovers, industrial AGVs, warehouse quadrupeds) run companion computers (Raspberry Pi 5, NVIDIA Jetson Orin Nano, embedded x86).

These robots run high-frequency sensors:
- IMU / Gyroscopes (100–500 Hz)
- LiDAR scans & point clouds (10–20 Hz)
- Motor current & wheel encoder odometry (50 Hz)
- Battery state & thermals (1 Hz)

### Critical Constraints
1. **Flash Storage Protection**: Embedded eMMC and SD cards have finite storage and write lifecycles. An unbounded log will brick the robot by filling `/var/log` or the root partition.
2. **Zero In-Process Contention**: High-frequency telemetry logging must never steal CPU cycles from the critical obstacle-avoidance or motor-control PID loops.
3. **Black-Box Crash Forensics**: If the robot crashes or power cuts abruptly, the exact telemetry leading up to the incident must be recoverable on-disk without corruption.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 Robotics Companion Sensors                  │
│       IMU (200Hz)      LiDAR (20Hz)      Odometry (50Hz)    │
└──────────────────────────────┬──────────────────────────────┘
                               │ publish("robot/sensor/<type>")
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - CRC32 integrity check     │
                │  - Automated retention prune │
                │  - Zero dependencies         │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                                           ▼
┌─────────────────┐                         ┌─────────────────┐
│ Real-Time Flight│                         │ Black-Box Flash │
│ Controller      │                         │ Recorder Log    │
│ (Reads hot RAM) │                         │ (Max 500 MiB)   │
└─────────────────┘                         └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import struct
import time
from pathlib import Path
from apister import Bus, OverflowPolicy


def pack_imu_reading(ax: float, ay: float, az: float, gx: float, gy: float, gz: float) -> bytes:
    # 6 doubles = 48 bytes
    return struct.pack("<6d", ax, ay, az, gx, gy, gz)


async def imu_sensor_loop(bus: Bus) -> None:
    """Publishes IMU telemetry at 100 Hz."""
    for _ in range(100):
        data = pack_imu_reading(0.01, -0.02, 9.81, 0.0, 0.01, -0.01)
        await bus.publish("robot/telemetry/imu", data)
        await asyncio.sleep(0.01)


async def flight_controller_loop(bus: Bus) -> None:
    """Reads latest IMU reading with zero lag."""
    sub = await bus.subscribe("robot/telemetry/imu", queue_size=1)
    async for envelope in sub:
        # Zero-copy buffer read directly into struct unpacker
        ax, ay, az, gx, gy, gz = struct.unpack("<6d", envelope.memoryview())
        # Fast attitude calculation
        pass


async def main() -> None:
    # Bounded disk footprint: never exceeds 500 MB on the embedded flash
    async with await Bus.open(
        "data/robot_blackbox",
        backend="auto",  # Uses native Rust engine for sub-microsecond disk logging
        retain_bytes=500 * 1024 * 1024,  # Automatically unlinks oldest segments
        segment_bytes=16 * 1024 * 1024,  # 16 MiB segment files
        commit_every=256,
        overflow=OverflowPolicy.DROP_OLDEST,
    ) as bus:
        await asyncio.gather(
            imu_sensor_loop(bus),
            flight_controller_loop(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Robotics & Embedded Telemetry

1. **Deterministic Retention**: `retain_bytes=500MB` acts as a rolling black-box tape recorder. Older segments are pruned automatically as new ones are sealed.
2. **Crash-Safe Segment Recovery**: If the drone's battery disconnects mid-flight, `apister` validates segments on the next boot, truncates any partially written uncommitted byte tail, and verifies every record's CRC32.
