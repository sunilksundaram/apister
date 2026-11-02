# Guide 03: RLHF, DPO & PPO Rollout Trajectory Buffers

## Problem & Architectural Challenge

In Reinforcement Learning from Human/AI Feedback (RLHF) and Direct Preference Optimization (DPO):
- An **Actor Model** generates hundreds of rollouts (sequences of token IDs, attention masks, and action logits).
- To compute optimization gradients, these large rollout batches must be consumed simultaneously by:
  1. **The Reward Model**: Scores the completion quality.
  2. **The Reference Model**: Evaluates the base policy to compute the KL-divergence penalty.
  3. **The Value/Critic Network**: Estimates generalized advantage estimation (GAE) baselines.
  4. **The Policy Gradient Optimizer**: Updates the actor model weights.

### The In-Process Memory Crisis
A single rollout batch across 128 sequences of 2,048 tokens with `bfloat16` logits is **hundreds of megabytes**. Passing this through standard in-memory queues to 4 model worker coroutines duplicates multi-gigabyte tensors in host RAM, causing sudden Out-of-Memory (OOM) crashes on expensive multi-GPU nodes.

---

## Architectural Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    Actor Model Rollouts                      │
│        (Generates token sequences, logits, and masks)        │
└──────────────────────────────┬───────────────────────────────┘
                               │ publish("rl/rollout/batch_42", tensor_bytes)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Zero-copy tensor log      │
                │  - Hot tail cache in RAM     │
                │  - Segmented disk replay     │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  Reward Model   │   │ Reference Model │   │ Critic / Value  │
│  (Scores Batch) │   │ (KL Penalty)    │   │ (GAE Baselines) │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Topic Hierarchy Design

- `rl/rollout/<epoch>/<batch_id>`: Full rollout trajectory tensor bytes.
- `rl/scores/<epoch>/<batch_id>`: Output reward scores from the reward model.
- `rl/kl/<epoch>/<batch_id>`: Computed KL divergences from reference policy.
- `rl/gradients/<epoch>/<batch_id>`: Final loss and gradient statistics.

---

## Production Implementation

```python
import asyncio
from pathlib import Path
from apister import Bus, OverflowPolicy


async def actor_rollout_generator(bus: Bus) -> None:
    """Simulates an Actor GPU generating rollout batches."""
    for batch_id in range(10):
        # In real code: torch.save(tensor_dict, buffer) or safetensors.numpy.save()
        fake_tensor_payload = b"[TENSORS:TOKENS_AND_LOGITS]" * 50_000  # ~1.3 MB
        await bus.publish(f"rl/rollout/epoch_1/{batch_id}", fake_tensor_payload)
        print(f"[Actor] Generated rollout batch {batch_id}")
        await asyncio.sleep(0.05)


async def reward_model_worker(bus: Bus) -> None:
    """Computes rewards over rollouts."""
    sub = await bus.subscribe("rl/rollout/+/+", name="reward-worker")
    async for envelope in sub:
        # Deserialize tensor without copying payload
        # payload is served hot from apister tail cache
        await asyncio.sleep(0.04)  # GPU forward pass simulation
        print(f"  [Reward Model] Scored {envelope.topic}")
        await sub.ack(envelope)


async def reference_model_worker(bus: Bus) -> None:
    """Evaluates base model to enforce KL divergence penalty."""
    sub = await bus.subscribe("rl/rollout/+/+", name="ref-worker")
    async for envelope in sub:
        await asyncio.sleep(0.06)  # GPU forward pass simulation
        print(f"  [Reference Model] KL-scored {envelope.topic}")
        await sub.ack(envelope)


async def main() -> None:
    async with await Bus.open(
        "data/rlhf_bus",
        tail_cache_bytes=256 * 1024 * 1024,  # 256 MiB hot buffer for recent tensors
        overflow=OverflowPolicy.BLOCK,  # Stalls actor if scoring models lag
        retain_bytes=2 * 1024 * 1024 * 1024,  # Keep 2 GiB of historical rollouts on disk
    ) as bus:
        await asyncio.gather(
            actor_rollout_generator(bus),
            reward_model_worker(bus),
            reference_model_worker(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` is the Right Fit

1. **Host RAM Protection**: Serving multiple evaluation workers from a single cached ledger entry prevents OOM crashes when batch sizes or sequence lengths grow.
2. **Crash-Resilient Spot Instance Recovery**: Cloud GPU instances (e.g. AWS Spot or GCP Preemptible VMs) can terminate abruptly. `apister`'s on-disk log and persistent cursors mean training can checkpoint and resume rollouts seamlessly.
