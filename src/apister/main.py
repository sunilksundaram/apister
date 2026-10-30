"""Demo: fan-out, wildcards, a durable ledger, and a broker that drops once."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from apister.brokers import MemoryBroker
from apister.bus import Bus
from apister.types import OverflowPolicy, ReconnectPolicy, SubscriptionClosed


async def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="apister-"))
    broker = MemoryBroker(fail_after=1)
    policy = ReconnectPolicy(initial_delay_s=0.01, max_delay_s=0.05, jitter_ratio=0)

    async with await Bus.open(
        root,
        max_queue_size=8,
        overflow=OverflowPolicy.BLOCK,
        segment_bytes=64 * 1024,
        commit_every=1,
    ) as bus:
        exact = await bus.subscribe("news/sports/final")
        wild = await bus.subscribe("news/#")
        await bus.attach_broker(broker, patterns=["news/#"], policy=policy)

        await bus.publish("news/sports/final", b"game over")
        await bus.publish("news/weather", b"clear")

        for subscription, expected in (
            (exact, [b"game over"]),
            (wild, [b"game over", b"clear"]),
        ):
            for _payload in expected:
                envelope = await subscription.get()
                print(f"{subscription.pattern} <- {envelope.topic} {envelope.payload!r}")

        for _ in range(100):
            if len(broker.sent) >= 2 and broker.connects >= 2:
                break
            await asyncio.sleep(0.01)
        print(f"broker connects={broker.connects} forwarded={len(broker.sent)}")

        await bus.unsubscribe(exact)
        try:
            await exact.get()
        except SubscriptionClosed:
            print("exact subscriber stopped on sentinel")

    reopened = await Bus.open(root, commit_every=1)
    try:
        replay = await reopened.subscribe("news/#", replay_from=0)
        seen = 0
        while seen < 2:
            envelope = await asyncio.wait_for(replay.get(), timeout=2)
            seen += 1
            print(f"replay {envelope.offset} {envelope.payload!r}")
        print(f"ledger={reopened.stats().ledger_bytes} bytes at {root}")
    finally:
        await reopened.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
