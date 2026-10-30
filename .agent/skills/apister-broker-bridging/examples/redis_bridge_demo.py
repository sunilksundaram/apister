"""Example: Attaching a Redis broker bridge to apister with custom backoff."""

from __future__ import annotations

import asyncio
from pathlib import Path
from apister import Bus, ReconnectPolicy
from apister.brokers import RedisBroker


async def main() -> None:
    data_dir = Path("data/redis-bridge-demo")

    async with await Bus.open(data_dir) as bus:
        # Create zero-dependency Redis broker adapter
        redis_broker = RedisBroker("redis://127.0.0.1:6379/0")

        # Define reconnection policy with exponential backoff and jitter
        policy = ReconnectPolicy(
            initial_delay_s=0.1,
            max_delay_s=10.0,
            multiplier=2.0,
            jitter_ratio=0.25,
            max_attempts=10,
        )

        try:
            # Attach broker to forward and ingest "notifications/#"
            await bus.attach_broker(
                redis_broker,
                patterns=["notifications/#"],
                policy=policy,
            )
            print("Redis broker attached successfully")

            # Local publish: automatically forwarded to Redis
            await bus.publish("notifications/email", b'{"to": "user@example.com"}')

            # Give forwarder pump a brief moment
            await asyncio.sleep(0.5)

            stats = bus.stats()
            print(f"Stats - Forwards: {stats.broker_forwards}, Reconnects: {stats.reconnects}")
        except Exception as exc:
            print(f"Could not connect to live Redis instance ({exc}); check broker availability.")


if __name__ == "__main__":
    asyncio.run(main())

