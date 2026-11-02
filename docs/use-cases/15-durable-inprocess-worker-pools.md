# Guide 15: Durable In-Process Job Queues & Worker Pools

## Problem & Architectural Challenge

Developers often need background worker pools to execute async tasks within a single Python service (e.g. sending transactional emails, processing uploaded PDF reports, indexing user uploads, or generating thumbnails):
- Using Celery or RQ requires spinning up external message brokers (Redis or RabbitMQ) and separate worker processes, adding operational complexity and deployment friction.
- Using standard `asyncio.Queue` introduces a catastrophic reliability gap: **if the Python process is redeployed, crashes, or reboots, every uncompleted job sitting in the in-memory queue is permanently lost**.

### The Solution: Zero-Broker Durable Worker Pools
`apister` gives you durable, at-least-once job queues right inside your Python application without running Redis, RabbitMQ, or Celery.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 FastAPI / AIOHTTP Web Handlers              │
│               (Enqueues background task requests)           │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish("tasks/emails/send", task_json)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Persistent on-disk ledger │
                │  - Monotonic ack cursors     │
                │  - Zero external brokers     │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Worker Coroutine│   │ Worker Coroutine│   │ Worker Coroutine│
│ Pool Member 1   │   │ Pool Member 2   │   │ Pool Member 3   │
│ (Acks on Finish)│   │ (Acks on Finish)│   │ (Acks on Finish)│
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def web_request_handler(bus: Bus) -> None:
    """Simulates HTTP endpoints enqueueing background tasks."""
    for job_id in range(10):
        job = {
            "job_id": f"job_{job_id}",
            "type": "send_welcome_email",
            "recipient": f"user_{job_id}@example.com",
        }
        await bus.publish("tasks/email", json.dumps(job).encode("utf-8"))
        print(f"[Web API] Enqueued job_{job_id}")


async def background_worker(bus: Bus, worker_id: int) -> None:
    """Async worker: acknowledges tasks with out-of-order completion safety."""
    sub = await bus.subscribe(
        "tasks/email",
        name=f"worker-node-{worker_id}",
        inflight_limit=4,  # Concurrency limit per worker
    )
    async for envelope in sub:
        job = json.loads(envelope.payload.decode())
        print(f"  [Worker {worker_id}] Executing {job['job_id']}...")

        try:
            # Simulate job execution (e.g. SMTP email send):
            await asyncio.sleep(0.05)
            # Acknowledge task completion: advances cursor on disk
            await sub.ack(envelope)
            print(f"  [Worker {worker_id}] Finished {job['job_id']}")
        except Exception as exc:
            # If an exception occurs, do NOT ack!
            # Upon process restart, apister redelivers this exact message
            print(f"  [Worker {worker_id}] Job failed: {exc}")


async def main() -> None:
    async with await Bus.open(
        "data/worker_bus",
        overflow=OverflowPolicy.BLOCK,
        commit_every=1,  # Immediate fsync for guaranteed task durability
    ) as bus:
        # Run 2 concurrent worker coroutines
        await asyncio.gather(
            web_request_handler(bus),
            background_worker(bus, worker_id=1),
            background_worker(bus, worker_id=2),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Out-of-Order Acknowledgments & Gap Tracking

When multiple workers execute tasks concurrently, tasks will complete out of order (e.g. Task 3 finishes before Task 2).

`apister` automatically handles this:
- If Task 3 is acknowledged before Task 2, `apister` remembers that Task 3 is complete, but **does not advance the persistent on-disk cursor past Task 2**.
- Once Task 2 finishes and calls `await sub.ack()`, the cursor leaps forward across the entire contiguous block.
- If the server power cuts while Task 2 is still running, `apister` on restart will redeliver from Task 2 without losing unfinished work.
