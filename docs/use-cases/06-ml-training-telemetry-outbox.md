# Guide 06: ML Training Telemetry & Metric Outbox

## Problem & Architectural Challenge

During long-running multi-GPU training jobs (e.g. pre-training, fine-tuning with DeepSpeed, Megatron, or PyTorch FSDP):
- The training loop computes step loss, perplexity, learning rate, gradient norms, and layer-by-layer activation statistics every few hundred milliseconds.
- Pushing these metrics synchronously across the network to logging dashboards (Weights & Biases, MLflow, Comet, TensorBoard) introduces a dangerous point of failure:
  - If a network blip occurs, the GPU training loop blocks on network I/O, idling expensive GPU clusters costing tens of dollars an hour.
  - If the cloud logging endpoint rate-limits or throws HTTP 500 errors, unhandled exceptions can crash the entire training job.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 GPU Training Step Loop                      │
│        (PyTorch / HuggingFace Trainer / DeepSpeed)          │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish("train/metrics/step_120", json_bytes)
                               │ Non-blocking in-memory write (< 50 microseconds)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Append-only on-disk log   │
                │  - Zero network wait         │
                └──────────────┬───────────────┘
                               │
                               ▼
                ┌──────────────────────────────┐
                │      Bridge Outbox Worker    │
                │  (Redis / Kafka / WandB Pump)│
                │  - Exponential backoff       │
                │  - Automatic reconnection    │
                └──────────────┬───────────────┘
                               │
                               ▼
                  External Monitoring Cloud
                  (WandB / TensorBoard / MLflow)
```

---

## Production Implementation

```python
import asyncio
import json
import time
from pathlib import Path
from apister import Bus, OverflowPolicy, ZstdCodec
from apister.brokers import RedisBroker
from apister.types import ReconnectPolicy


async def gpu_training_step_loop(bus: Bus) -> None:
    """Simulates an inner multi-GPU training loop publishing compressed metric batches."""
    codec = ZstdCodec(level=3)

    for step in range(1, 100):
        # Heavy GPU tensor computation simulation:
        await asyncio.sleep(0.02)

        metrics = {
            "step": step,
            "loss": 2.45 - (step * 0.01),
            "grad_norm": 0.85,
            "timestamp": time.time(),
        }
        compressed_payload = codec.compress(json.dumps(metrics).encode("utf-8"))

        # Batch publish step loss, gradient stats, and optimizer states atomically:
        await bus.publish_many(
            [
                (f"train/metrics/step_{step}", compressed_payload),
                (f"train/gradients/step_{step}", compressed_payload),
            ]
        )
        print(f"[GPU Step {step}] Step completed and metric batch logged locally")


async def main() -> None:
    storage = Path("data/training_telemetry")
    async with await Bus.open(
        storage,
        overflow=OverflowPolicy.DROP_OLDEST,  # Ensures GPU loop NEVER blocks
        commit_every=64,
        segment_bytes=16 * 1024 * 1024,
        disk_warning_threshold=0.85,  # Alert before multi-day training saturates SSD storage
    ) as bus:
        # Attach external bridge to central cluster Redis / Kafka
        # Automatically retries with exponential backoff if remote broker is down
        redis_broker = RedisBroker("redis://127.0.0.1:6379/0")
        try:
            await bus.attach_broker(
                redis_broker,
                patterns=["train/metrics/#"],
                policy=ReconnectPolicy(
                    initial_delay_s=0.2,
                    max_delay_s=10.0,
                    multiplier=2.0,
                ),
            )
        except Exception:
            print("[Warning] Remote broker offline. Telemetry buffered safely to local disk.")

        await gpu_training_step_loop(bus)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Training Telemetry

1. **Decoupled Failure Domains**: Even if the cloud monitoring service or corporate VPN goes down completely for two hours, the GPU training loop continues uninterrupted at full speed.
2. **Offline-Safe Store-and-Forward**: Every single step's metrics are written to local disk segments. When connectivity is restored, the built-in outbox pump uploads the backlog in chronological order.
