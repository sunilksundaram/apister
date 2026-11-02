# Guide 18: Multi-Camera Edge NVR & Surveillance Vision

## Problem & Architectural Challenge

Network Video Recorders (NVR) and smart security edge devices process live RTSP/WebRTC video streams from 4 to 16 IP cameras concurrently:
- Each camera yields 25–30 frames per second at 1080p or 4K resolution.
- Each incoming frame must be processed by multiple consumers simultaneously:
  1. **Live UI WebSockets**: Low-latency MJPEG/H.264 video feed for security monitoring dashboards.
  2. **Motion Detection Worker**: Fast optical flow or background subtraction on CPU to trigger recording clips.
  3. **Deep Learning Vision Engine**: Heavy GPU model (YOLOv10 / FaceNet) executed only when motion is flagged.
  4. **Continuous Ring Buffer**: Writing raw frames into temporary rolling storage on disk for 24-hour review.

### The Pitfalls
- **Memory Explosion**: Passing uncompressed 1080p frames (~6 MB per raw RGB frame) through standard multi-consumer queues rapidly exhausts edge system RAM and triggers the Linux Out-Of-Memory (OOM) killer.
- **Backpressure Deadlocks**: If the deep learning GPU model experiences latency spikes, it must not cause the live security monitor or disk recorder to freeze.

### The Solution: Zero-Copy Shared Memory & Wildcard Topics
`apister` delivers frames via zero-copy byte offsets. MQTT topic wildcards (`camera/+/frames`) allow workers to subscribe selectively, while independent subscriber queues prevent slow models from blocking live feeds.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 RTSP Camera Ingest Coroutines               │
│          camera/front/frames, camera/warehouse/frames       │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish(topic, raw_frame)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Zero memory amplification │
                │  - Topic wildcard routing    │
                │  - LRU hot tail-cache        │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Live UI Feed    │   │ Motion Detector │   │ Rolling Storage │
│ (Sub: "camera/  │   │ (Sub: "camera/  │   │ (Sub: "camera/  │
│   front/frames")│   │   +/frames")    │   │   +/frames")    │
│ [Drop Oldest]   │   │ [CPU Optimized] │   │ [Disk Archiver] │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
from pathlib import Path
from apister import Bus, OverflowPolicy


async def rtsp_camera_streamer(bus: Bus, camera_id: str, frame_count: int = 15) -> None:
    """Simulates RTSP video capture emitting uncompressed frames."""
    # Simulated 1080p RGB frame header + payload:
    dummy_frame = b"FRAME_HEADER_V1:" + (
        b"\xaa\xbb\xcc" * 1024
    )  # Lightweight representative payload

    for frame_idx in range(frame_count):
        topic = f"camera/{camera_id}/frames"
        await bus.publish(topic, dummy_frame)
        await asyncio.sleep(0.033)  # ~30 FPS


async def motion_detection_worker(bus: Bus) -> None:
    """Listens to all cameras using MQTT wildcard `camera/+/frames` with zero copy."""
    sub = await bus.subscribe("camera/+/frames", name="motion_detector", inflight_limit=32)
    async for envelope in sub:
        # Extract camera_id from topic:
        parts = envelope.topic.split("/")
        cam_id = parts[1]

        # Zero-copy memoryview slice avoids copying 1080p frame bytes:
        frame_slice = envelope.memoryview()
        has_motion = envelope.offset % 5 == 0  # Simulated motion event
        if has_motion:
            print(f"  [Motion Detector] Motion detected on {cam_id} at offset {envelope.offset}!")
            # Trigger high-priority neural alert:
            await bus.publish(f"alerts/{cam_id}/motion", b"OBJECT_DETECTED")

        await sub.ack(envelope)


async def live_dashboard_streamer(bus: Bus) -> None:
    """Streams live front-door feed to web dashboard; drops old frames if UI lags."""
    sub = await bus.subscribe("camera/front_entrance/frames")
    processed = 0
    async for envelope in sub:
        processed += 1
        # Zero-copy buffer slicing directly into WebSocket or frame streamer
        _frame_view = envelope.memoryview()
        if processed >= 15:
            break


async def main() -> None:
    storage = Path("data/nvr_ledger")
    async with await Bus.open(
        storage,
        backend="auto",
        segment_bytes=16 * 1024 * 1024,
        retain_bytes=500 * 1024 * 1024,
        overflow=OverflowPolicy.DROP_OLDEST,
        tail_cache_bytes=32 * 1024 * 1024,
    ) as bus:
        await asyncio.gather(
            rtsp_camera_streamer(bus, "front_entrance", frame_count=15),
            rtsp_camera_streamer(bus, "warehouse_bay", frame_count=15),
            motion_detection_worker(bus),
            live_dashboard_streamer(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Best Practices & Tuning

1. **`tail_cache_records` Sizing**: Set `tail_cache_records` to hold at least 2–3 seconds of frames (e.g. `256` or `512` records). Live UI monitors and real-time motion detectors will read 100% of frames directly from memory without touching disk storage.
2. **`OverflowPolicy.DROP_OLDEST`**: In video surveillance, current real-time frames are more critical than stale historical frames; dropping the oldest unread frames prevents latency accumulation.
3. **Multi-Camera Wildcards**: Using `camera/+/frames` enables adding and removing cameras dynamically without altering the motion-detection worker code.
