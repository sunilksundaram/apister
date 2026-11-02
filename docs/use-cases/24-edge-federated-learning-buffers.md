# Guide 24: Edge Federated Learning Gradient Buffers & Cloud Outbox

## Problem & Architectural Challenge

Federated Learning (FL) trains machine learning models collaboratively across edge devices (smartphones, hospital diagnostic equipment, autonomous vehicles, retail kiosks) without centralizing raw private data:
- Edge nodes compute local parameter updates (weight deltas or gradient tensors) over private datasets every few minutes.
- The device cannot immediately transmit these heavy tensors to the cloud due to:
  1. **Intermittent Connectivity**: Moving vehicles or remote medical clinics experience frequent cellular drops.
  2. **Bandwidth Costs & Metering**: Data uploads should wait for unmetered Wi-Fi connections or off-peak power hours.
  3. **Coordinator Synchronization**: The central aggregation server (e.g. Flower, PySyft) only opens upload rounds periodically.

### The Pitfalls
- **Lost Training Work**: Storing multi-megabyte tensor updates in volatile memory risks wiping out hours of edge training computation if the device restarts.
- **Network Queue Blocking**: If gradient upload fails, the local training loop must not be blocked or crash.

### The Solution: Local Durable Outbox with Broker Bridge Forwarding
`apister` buffers local gradient tensors safely into an on-disk segmented ledger. When the central federation server requests an update and network connectivity is active, `apister`’s Redis, RabbitMQ, or Kafka broker bridge forwards accumulated gradients with loop-free deduplication and at-least-once reliability.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 Local On-Device Training Loop               │
│                 (PyTorch / TensorFlow Lite / JAX)           │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish("fl/client42/gradients", weights)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Disk-backed outbox        │
                │  - Strict byte retention     │
                │  - Durable cursor state      │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┴─────────────────────┐
         ▼ (When Wi-Fi connected & FL round active)  ▼
┌──────────────────────────────┐            ┌──────────────────────────────┐
│ apister Cloud Broker Bridge  │            │ Central Federated Server     │
│ (Redis / Kafka / RabbitMQ)   │ ─────────> │ (Aggregates FedAvg / FedProx │
│  - Loop-free deduplication   │            │  global model checkpoint)    │
└──────────────────────────────┘            └──────────────────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def edge_training_worker(bus: Bus, client_id: str) -> None:
    """Computes local gradient updates on private training batches."""
    for epoch in range(5):
        # Simulated gradient tensor delta payload (e.g. state_dict diff):
        gradient_payload = {
            "client_id": client_id,
            "epoch": epoch,
            "loss": 0.35 - (epoch * 0.04),
            "weights_sample": [0.012, -0.045, 0.129, -0.008, 0.051],
        }
        await bus.publish(f"fl/{client_id}/gradients", json.dumps(gradient_payload).encode("utf-8"))
        print(f"[Edge Trainer] Completed epoch {epoch}; buffered gradient delta to local outbox.")
        await asyncio.sleep(0.02)


async def cloud_federation_outbox_bridge(bus: Bus, client_id: str) -> None:
    """Streams local gradient buffer to central cloud broker when network is available."""
    sub = await bus.subscribe(f"fl/{client_id}/gradients", name="cloud_sync_cursor")

    uploaded = 0
    async for envelope in sub:
        delta = json.loads(envelope.payload.decode())

        # Simulate network check & upload to central cloud broker:
        is_wifi_available = True
        if is_wifi_available:
            await asyncio.sleep(0.01)  # Simulate cloud upload latency
            uploaded += 1
            print(
                f"  [Cloud Bridge] Uploaded epoch {delta['epoch']} gradients to central aggregator."
            )
            await sub.ack(envelope)

        if uploaded >= 5:
            break


async def main() -> None:
    storage = Path("data/edge_federated_outbox")
    async with await Bus.open(
        storage,
        segment_bytes=16 * 1024 * 1024,
        retain_bytes=200 * 1024 * 1024,  # Bounded to 200 MB on edge storage
        overflow=OverflowPolicy.DROP_OLDEST,
    ) as bus:
        client_id = "hospital_node_09"
        await asyncio.gather(
            edge_training_worker(bus, client_id),
            cloud_federation_outbox_bridge(bus, client_id),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Best Practices & Tuning

1. **`retain_bytes` Flash Limit**: Set an explicit disk retention budget (e.g., `200 * 1024 * 1024`) so gradient checkpoints never fill the host operating system drive during extended network outages.
2. **Crash-Proof Resume**: If the edge device restarts midway through a training cycle, `cloud_sync_cursor` resumes from the exact un-uploaded gradient epoch.
3. **Loop-Free Deduplication**: When paired with `apister`'s Redis or Kafka broker bridges, deduplication digests prevent re-uploading the same gradient delta twice upon reconnect.
