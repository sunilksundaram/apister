# Guide 21: Database Change Data Capture (CDC) Staging & Outbox

## Problem & Architectural Challenge

Change Data Capture (CDC) pipelines stream real-time database mutations (INSERT, UPDATE, DELETE) directly from write-ahead logs (e.g. PostgreSQL logical replication WAL or MySQL binary logs):
- Mutations must be fanned out to multiple downstream targets:
  1. **Cache Invalidator**: Purges Redis/Memcached entries immediately.
  2. **Elasticsearch / Meilisearch Sync**: Updates search indices.
  3. **Third-Party Webhooks**: Dispatches outbound HTTP notifications (e.g. Stripe, Zapier, partner APIs).
  4. **Data Lake Archiver**: Writes parquet batches into S3/GCS.

### The Pitfalls
- **Primary Database WAL Bloat**: PostgreSQL replication slots will accumulate un-vacuumed WAL files if a downstream consumer (like an external webhook with network rate limits) stalls. If WAL storage reaches 100%, **the primary production database will stop accepting writes and crash**.
- **Coupled Outage Risk**: If Elasticsearch goes down, the entire CDC ingestion pipeline stalls unless decoupled.

### The Solution: Local Staging Ledger with Decoupled Subscriber Speeds
`apister` acts as a durable staging buffer. The CDC tailer reads the database WAL and immediately appends to `apister`, instantly advancing the database replication slot. Fast subscribers (cache invalidators) process events instantly, while slow or rate-limited subscribers (webhooks) catch up at their own pace without impacting the production database.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 PostgreSQL Logical Replication              │
│                (Tails WAL, pushes table change events)       │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish("cdc/users/update", change)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Relieves DB WAL pressure  │
                │  - Independent worker rates  │
                │  - Persistent ACK cursors    │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Fast: Redis     │   │ Medium: Search  │   │ Slow: External  │
│ Invalidator     │   │ Engine Indexer  │   │ Partner Webhooks│
│ (Sub: "cdc/#")  │   │ (Sub: "cdc/#")  │   │ (Sub: "cdc/#")  │
│ [Instant Drain] │   │ [Bulk Batched]  │   │ [Rate-Limited]  │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def postgres_wal_tailer(bus: Bus) -> None:
    """Tails PostgreSQL logical replication stream and enqueues to apister."""
    for i in range(15):
        cdc_event = {
            "lsn": f"0/16B{i:04X}",
            "table": "users",
            "operation": "UPDATE",
            "data": {"id": i, "email": f"user{i}@example.com", "verified": True},
        }
        await bus.publish("cdc/tables/users", json.dumps(cdc_event).encode("utf-8"))
        await asyncio.sleep(0.01)
    print("[PostgreSQL CDC] Ingested 15 WAL events and acknowledged replication slot.")


async def fast_cache_invalidator(bus: Bus) -> None:
    """Fast subscriber: immediately purges cache entries."""
    sub = await bus.subscribe("cdc/tables/+", name="cache_invalidator")
    count = 0
    async for envelope in sub:
        event = json.loads(envelope.payload.decode())
        count += 1
        # Instant Redis cache delete:
        # await redis.delete(f"user:{event['data']['id']}")
        await sub.ack(envelope)
        if count >= 15:
            break
    print(f"  [Cache Invalidator] Successfully invalidated {count} cached keys.")


async def slow_partner_webhook_dispatcher(bus: Bus) -> None:
    """Slow rate-limited subscriber: sends external HTTP webhooks."""
    sub = await bus.subscribe("cdc/tables/+", name="webhook_dispatcher", inflight_limit=4)
    processed = 0
    async for envelope in sub:
        event = json.loads(envelope.payload.decode())
        processed += 1
        # Simulate outbound HTTP request with rate limit / network delay:
        await asyncio.sleep(0.04)
        print(f"    [Webhook HTTP] Dispatched partner webhook for user {event['data']['id']}")
        await sub.ack(envelope)
        if processed >= 15:
            break


async def main() -> None:
    storage = Path("data/cdc_staging_ledger")
    async with await Bus.open(
        storage,
        segment_bytes=16 * 1024 * 1024,
        retain_bytes=1024 * 1024 * 1024,  # 1 GB buffer
        overflow=OverflowPolicy.BLOCK,
    ) as bus:
        await asyncio.gather(
            postgres_wal_tailer(bus),
            fast_cache_invalidator(bus),
            slow_partner_webhook_dispatcher(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Best Practices & Tuning

1. **Protect Database Health**: By shifting downstream delivery from the database's replication connection to `apister`, network hiccups on external webhooks will never exhaust Postgres disk space or stall the primary DB.
2. **Crash-Safe Cursors**: If the webhook sender crashes halfway through sending events, restarting the process automatically resumes at the unacknowledged offset without re-running completed webhooks.
3. **Partition by Table**: Using topics like `cdc/tables/{table_name}` allows subscribers to filter only the entities they care about.

