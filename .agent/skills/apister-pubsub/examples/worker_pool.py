"""Example: Durable worker pool with persistent acknowledgment cursors."""

from __future__ import annotations

import asyncio
from pathlib import Path
from apister import Bus, SubscriptionClosed


async def worker(worker_id: str, bus: Bus, pattern: str) -> None:
    sub = await bus.subscribe(pattern, name=worker_id, inflight_limit=4)
    print(f"[{worker_id}] Subscribed to {pattern} with inflight_limit=4")

    try:
        async for envelope in sub:
            print(f"[{worker_id}] Processing: {envelope.topic} -> {envelope.payload.decode()}")
            # Simulate async task work
            await asyncio.sleep(0.05)
            # Acknowledge task completion
            await sub.ack(envelope)
            print(f"[{worker_id}] Acked offset {envelope.offset}")
    except SubscriptionClosed:
        print(f"[{worker_id}] Subscription closed")


async def main() -> None:
    data_dir = Path("data/demo-pool")
    async with await Bus.open(data_dir, commit_every=1) as bus:
        # Start two worker instances with distinct persistent cursors
        w1 = asyncio.create_task(worker("worker-1", bus, "tasks/#"))
        w2 = asyncio.create_task(worker("worker-2", bus, "tasks/#"))

        # Produce tasks
        for i in range(10):
            await bus.publish(f"tasks/item-{i}", f"payload-{i}".encode("utf-8"))
            await asyncio.sleep(0.02)

        # Allow workers to finish
        await asyncio.sleep(1.0)
        await bus.shutdown()
        await asyncio.gather(w1, w2)


if __name__ == "__main__":
    asyncio.run(main())

