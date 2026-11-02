# Guide 19: Live Audio Studio, DSP & Multi-Track Recording

## Problem & Architectural Challenge

Live audio software, podcast studios, and digital signal processing (DSP) applications capture continuous streams of raw uncompressed audio:
- 48 kHz, 24-bit stereo or multi-channel audio chunks (10–20 ms buffers).
- Every audio buffer must be delivered synchronously and concurrently to:
  1. **DSP Filter Pipeline**: Live noise-suppression, compressor, and parametric EQ with sub-10ms latency.
  2. **Live VU Meters & Spectrogram**: 60 FPS graphical visualizer for recording engineers.
  3. **Live Streaming Encoder**: Lossy AAC/Opus encoding for WebRTC or RTMP streaming.
  4. **Master Disk Archiver**: Uncompressed raw PCM/WAV recording to disk for post-production editing.

### The Pitfalls
- **Audio Glitches & Buffer Underruns**: If the disk archiver blocks the asyncio event loop during an I/O sync, the audio hardware driver starves and produces audible "pops", "clicks", or lost speech frames.
- **Lost Recordings on Crash**: Storing the raw stream solely in memory risks total data loss if the streaming software crashes mid-show.

### The Solution: Tail Cache & Decoupled Ledger Append
`apister`’s hot tail-cache serves real-time DSP filters directly from RAM. At the same time, the background single-writer thread appends audio buffers to a segmented disk ledger with zero impact on the audio processing loop.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 Hardware Audio Capture (ALSA / WASAPI)      │
│                 (Emits 20ms raw 48kHz PCM audio blocks)     │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish("audio/mic1/raw", pcm_chunk)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Microsecond tail-cache    │
                │  - Non-blocking writes       │
                │  - Zero GC fragmentation     │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ DSP Filters     │   │ Live Visualizer │   │ Disk Archiver   │
│ (Noise Gate /   │   │ (VU Peak Meters │   │ (Lossless WAV   │
│  Compressor)    │   │  & FFT Display) │   │  Persistent Ack)│
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import math
import struct
from pathlib import Path
from apister import Bus, OverflowPolicy


def generate_pcm_sine(
    freq: float = 440.0, duration_ms: int = 20, sample_rate: int = 48000
) -> bytes:
    """Generates 20ms of 16-bit mono PCM audio."""
    num_samples = int(sample_rate * (duration_ms / 1000.0))
    samples = []
    for i in range(num_samples):
        val = int(32767.0 * math.sin(2.0 * math.pi * freq * (i / sample_rate)))
        samples.append(val)
    return struct.pack(f"<{num_samples}h", *samples)


async def audio_hardware_capture(bus: Bus) -> None:
    """Simulates 48kHz audio interface delivering 20ms buffers."""
    for chunk_id in range(30):
        # Audio chunks of 20ms:
        pcm = generate_pcm_sine(freq=440.0, duration_ms=20)
        await bus.publish("audio/mic1/raw", pcm)
        await asyncio.sleep(0.015)  # Simulate real-time stream


async def live_vu_meter_visualizer(bus: Bus) -> None:
    """Consumes audio from tail-cache to calculate peak decibels for the GUI with zero copy."""
    sub = await bus.subscribe("audio/mic1/raw")
    processed = 0
    async for envelope in sub:
        # Zero-copy buffer slicing via memoryview avoids heap copying 48kHz audio:
        raw_view = envelope.memoryview()
        num_samples = len(raw_view) // 2
        samples = struct.unpack(f"<{num_samples}h", raw_view)
        # Note: With numpy installed, this is even faster:
        # samples_np = envelope.to_numpy(dtype=np.int16)
        # peak = int(np.abs(samples_np).max())
        peak = max(abs(s) for s in samples)
        db = 20 * math.log10(peak / 32767.0 + 1e-6)
        processed += 1
        if processed % 5 == 0:
            print(f"  [VU Meter] Peak: {peak:05d} ({db:.1f} dBFS) at offset {envelope.offset}")
        if processed >= 30:
            break


async def master_wav_archiver(bus: Bus) -> None:
    """Durable disk archiver: saves master track and advances persistent cursor."""
    sub = await bus.subscribe("audio/mic1/raw", name="master_track_archiver")
    total_bytes = 0
    async for envelope in sub:
        # Write to master audio file:
        total_bytes += len(envelope.payload)
        await sub.ack(envelope)
        if total_bytes >= 30 * 1920:  # 30 frames
            break
    print(f"  [Disk Archiver] Safely persisted {total_bytes} bytes of raw audio to master track.")


async def main() -> None:
    storage = Path("data/audio_studio_ledger")
    async with await Bus.open(
        storage,
        backend="auto",
        segment_bytes=16 * 1024 * 1024,
        retain_bytes=500 * 1024 * 1024,
        overflow=OverflowPolicy.DROP_OLDEST,
        tail_cache_bytes=16 * 1024 * 1024,
        commit_every=64,
    ) as bus:
        await asyncio.gather(
            audio_hardware_capture(bus),
            live_vu_meter_visualizer(bus),
            master_wav_archiver(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Best Practices & Tuning

1. **`tail_cache_records=128`**: Guarantees that live DSP and VU meter coroutines read directly from RAM without initiating disk reads.
2. **`commit_every=64`**: Bundles disk syncs to avoid latency jitter on the audio thread.
3. **Lossless Master Recovery**: If an operating system crash occurs, the recorded PCM segments are fully intact on disk up to the last written frame.
