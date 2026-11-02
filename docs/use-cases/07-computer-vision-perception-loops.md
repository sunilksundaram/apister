# Guide 07: Computer Vision & Real-Time Perception Loops

## Problem & Architectural Challenge

Edge vision systems (e.g. smart security cameras, factory quality inspection, autonomous delivery robots, and traffic monitoring) ingest video streams at 30 to 60 FPS:
- A single 1080p uncompressed RGB frame is approximately **6 MB**.
- At 30 FPS, an ingestion camera thread ingests **180 MB per second**.

This high-throughput visual stream must be evaluated concurrently by multiple independent neural networks:
1. **Object Detection**: Fast YOLO / RT-DETR model (detecting pedestrians, vehicles, defects).
2. **OCR / License Plate Recognition**: Crop-and-read text detection.
3. **Face / Biometric Recognition**: Embeddings for access control.
4. **Archival Clip Recorder**: Writes compressed MP4/H.264 video to storage upon anomaly detection.

### The Problem with Python Queues
If an application pushes a 6 MB frame to 4 standard `asyncio.Queue` instances, Python duplicates 24 MB per frame (over **720 MB/sec** of memory allocations). The Python memory manager and garbage collector choke, causing dropped frames and high inference latency.

---

## Architectural Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    Camera Ingestion Thread                   │
│               (Capture 30 FPS 1080p Frames)                  │
└──────────────────────────────┬───────────────────────────────┘
                               │ publish("cam/main/raw_frame", frame_bytes)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Zero-Copy Ledger:         │
                │    Payload written 1x        │
                │  - Hot Tail Cache:           │
                │    Served from RAM buffer    │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┬─────────────────────┐
         ▼                     ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ YOLO Detector   │   │  License OCR    │   │ Face Recognition│   │ Anomaly Recorder│
│ (Real-Time 30Hz)│   │  (Sampled 5Hz)  │   │ (Triggered 2Hz) │   │ (Full Archive)  │
└─────────────────┘   └─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
from pathlib import Path
from apister import Bus, OverflowPolicy


async def camera_frame_producer(bus: Bus) -> None:
    """Simulates video frame ingestion from OpenCV / GStreamer."""
    fake_frame = b"\x89PNG\r\n\x1a\n" + (b"\x00\xff" * 500_000)  # ~1 MB frame
    for frame_id in range(30):
        # Frame written once to ledger; only 8-byte offset sent to subscribers
        await bus.publish("cam/front/frame", fake_frame)
        await asyncio.sleep(0.033)  # 30 FPS


async def yolo_detector_worker(bus: Bus) -> None:
    """Consumes every frame in real-time with zero memory allocations."""
    sub = await bus.subscribe("cam/front/frame", queue_size=2)
    async for envelope in sub:
        # Zero-copy slicing into buffer protocol or direct NumPy array:
        # frame_view = envelope.memoryview()
        # frame_tensor = envelope.to_numpy(dtype=np.uint8)
        # Payload served directly from apister Tail Cache!
        pass


async def anomaly_recorder_worker(bus: Bus) -> None:
    """Archival recorder: ensures zero frames are dropped for incident replay."""
    sub = await bus.subscribe("cam/front/frame", name="disk-recorder", queue_size=128)
    async for envelope in sub:
        # Persist frame to video buffer
        await sub.ack(envelope)


async def main() -> None:
    async with await Bus.open(
        "data/vision_bus",
        backend="auto",  # Hybrid Rust core for 1.26M writes/sec and mmap reads
        overflow=OverflowPolicy.DROP_OLDEST,  # Real-time detector drops old frames if GPU lags
        tail_cache_bytes=64 * 1024 * 1024,  # 64 MiB holds last ~60 frames in RAM
        retain_bytes=1024 * 1024 * 1024,  # 1 GiB on-disk ring buffer for incident extraction
    ) as bus:
        await asyncio.gather(
            camera_frame_producer(bus),
            yolo_detector_worker(bus),
            anomaly_recorder_worker(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Computer Vision

1. **Zero-Copy Memory Footprint**: No matter how many neural nets inspect the camera stream, the memory footprint remains constant.
2. **On-Disk Ring Buffer for Pre-Event Recording**: When an alarm trips (e.g., intruder detected), the application can query `replay_from=offset_30_seconds_ago` to instantly export the 30 seconds of footage preceding the alarm directly from disk segments.
