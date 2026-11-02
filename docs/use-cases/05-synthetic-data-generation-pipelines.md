# Guide 05: Synthetic Data Generation & Backpressure Pipelines

## Problem & Architectural Challenge

Creating high-quality instruction datasets (Self-Instruct, Evol-Instruct, Constitutional AI) involves generating hundreds of thousands of prompt-response pairs using large foundation models.

The generation workflow consists of distinct stages with radically different throughput rates:
1. **Generator Coroutines (Very Fast)**: Calls commercial LLM APIs concurrently to produce candidate instructions.
2. **De-duplication & Embedding Check (Moderate)**: Computes similarity against existing pairs.
3. **Execution Sandbox (Slow & Heavy)**: Executes generated Python/Bash scripts inside isolated sandboxes to verify syntax and tests.
4. **LLM-as-a-Judge Scorer (Slow)**: Runs verification prompts to rate alignment and clarity.

### The Buffer Overflow Problem
Without strict flow control, fast generation tasks flood the memory queue with candidate pairs while sandboxes and judge models lag behind. In a matter of minutes, thousands of unverified instructions queue up in RAM, leading to Out-of-Memory crashes and lost API spend.

---

## Architectural Flow

```
┌──────────────────────────────────────────────────────────────┐
│                  Synthetic Prompt Generators                 │
│         (High concurrency: 50 parallel API workers)          │
└──────────────────────────────┬───────────────────────────────┘
                               │ publish("synth/raw/code/task_42")
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - OverflowPolicy.BLOCK      │
                │  - Inflight Parking          │
                │  - Monotonic Ack Cursors     │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                                           ▼
┌─────────────────┐                         ┌─────────────────┐
│ Code Execution  │                         │ LLM Quality     │
│ Sandbox Worker  │                         │ Judge Worker    │
│ Cursor: "code"  │                         │ Cursor: "judge" │
└────────┬────────┘                         └────────┬────────┘
         │                                           │
         └─────────────────────┬─────────────────────┘
                               ▼
                  publish("synth/verified/#")
                               │
                               ▼
                ┌──────────────────────────────┐
                │  Durable JSONL / Parquet     │
                │  Dataset Exporter            │
                └──────────────────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def prompt_generator(bus: Bus) -> None:
    """Generates synthetic tasks at high speed."""
    for i in range(50):
        task_data = {
            "id": f"task_{i}",
            "prompt": f"Write a Python function to reverse linked list variant {i}",
            "response": f"def reverse_list_{i}(head): pass",
        }
        # Under OverflowPolicy.BLOCK, this awaits if downstream workers are full!
        await bus.publish("synth/raw/python", json.dumps(task_data).encode("utf-8"))
        print(f"[Generator] Emitted task {i}")


async def sandbox_execution_worker(bus: Bus) -> None:
    """Slow worker: executes code in sandbox to verify correctness."""
    sub = await bus.subscribe(
        "synth/raw/python",
        name="sandbox-evaluator",
        inflight_limit=4,  # Only allows 4 unacked jobs concurrently
        queue_size=8,  # Bounded queue triggers backpressure on generator
    )
    async for envelope in sub:
        data = json.loads(envelope.payload.decode())
        # Simulate isolated code execution latency:
        await asyncio.sleep(0.1)
        print(f"  [Sandbox] Verified {data['id']}")

        # Publish verified data forward:
        await bus.publish(f"synth/verified/{data['id']}", envelope.payload)
        await sub.ack(envelope)


async def dataset_exporter(bus: Bus) -> None:
    """Persists verified data to final training dataset."""
    sub = await bus.subscribe("synth/verified/+", name="dataset-exporter")
    async for envelope in sub:
        # Append to training jsonl
        await sub.ack(envelope)


async def main() -> None:
    async with await Bus.open(
        "data/synth_bus",
        overflow=OverflowPolicy.BLOCK,  # Essential: pauses generators when sandboxes are busy
        command_queue_size=64,
    ) as bus:
        await asyncio.gather(
            prompt_generator(bus),
            sandbox_execution_worker(bus),
            dataset_exporter(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Synthetic Data Pipelines

1. **Self-Balancing Generation Rate**: `OverflowPolicy.BLOCK` combined with `inflight_limit=4` guarantees the generator coroutine never runs faster than the validation sandbox can evaluate.
2. **Never Re-Generate on Failures**: Generating 500,000 synthetic pairs can take days. If an unhandled exception or system reboot occurs, named cursors ensure that already-verified tasks are never re-run.
