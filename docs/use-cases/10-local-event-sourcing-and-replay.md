# Guide 10: Local Event Sourcing, CQRS & Deterministic Replay

## Problem & Architectural Challenge

When developing rich local applications (e.g. Electron + Python backends, PyQt/PySide desktop GUIs, or terminal-based TUIs like Textual):
- Traditional relational state updates mutate rows in-place (`UPDATE users SET ...`).
- When a bug occurs, or when users request features like **Undo/Redo, Time-Travel Debugging, or Complete State Audit**, in-place mutation provides zero historical visibility.
- Rebuilding corrupted in-memory application caches requires complex database dump and restore scripts.

### The Event Sourcing Solution
Instead of storing current state, the application writes an immutable append-only ledger of domain events (`user/created`, `document/edited`, `tab/switched`). In-memory read models (projections) subscribe to the event stream to render UI views.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 Desktop UI / User Interaction               │
└──────────────────────────────┬──────────────────────────────┘
                               │ publish("events/doc/saved", event_bytes)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Immutable append-only log │
                │  - Segmented on-disk storage │
                │  - replay_from=offset        │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ In-Memory SQLite│   │ Search Indexer  │   │ Audit / Undo    │
│ Projection View │   │ (Full-text)     │   │ History Stack   │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus


async def user_ui_action_producer(bus: Bus) -> None:
    """Simulates user making edits in a desktop software application."""
    events = [
        {"action": "create_document", "id": "doc_1", "title": "Quarterly Plan"},
        {"action": "insert_text", "id": "doc_1", "text": "Q3 Revenue Target: $1M"},
        {"action": "tag_document", "id": "doc_1", "tag": "finance"},
    ]
    for event in events:
        topic = f"domain/document/{event['action']}"
        await bus.publish(topic, json.dumps(event).encode("utf-8"))
        print(f"[User Action] Published {topic}")


async def live_ui_projection_consumer(bus: Bus) -> None:
    """Maintains active UI view in memory."""
    sub = await bus.subscribe("domain/document/#")
    async for envelope in sub:
        event = json.loads(envelope.payload.decode())
        print(f"  -> [Live UI View] Rendered state change: {event['action']}")


async def main() -> None:
    db_path = Path("data/desktop_events")

    # Session 1: User performs actions
    print("=== SESSION 1: LIVE EVENT STREAMING ===")
    async with await Bus.open(db_path, commit_every=1) as bus:
        await user_ui_action_producer(bus)
        # In a real app, the consumer runs concurrently:
        sub = await bus.subscribe("domain/document/#", replay_from=0)
        envelope = await sub.get()
        print(f"Read event: {json.loads(envelope.payload)['action']}")

    # Session 2: App restarts, rebuilds full state from raw events at offset 0
    print("\n=== SESSION 2: FULL PROJECTION REBUILD FROM BYTE OFFSET 0 ===")
    reopened = await Bus.open(db_path, commit_every=1)
    try:
        replay_sub = await reopened.subscribe("domain/document/#", replay_from=0)
        reconstructed_doc = {}
        for _ in range(3):
            envelope = await replay_sub.get()
            event = json.loads(envelope.payload.decode())
            if event["action"] == "create_document":
                reconstructed_doc["title"] = event["title"]
            elif event["action"] == "insert_text":
                reconstructed_doc["body"] = event["text"]
            elif event["action"] == "tag_document":
                reconstructed_doc["tag"] = event["tag"]
            print(f"Replayed at offset {envelope.offset}: {event['action']}")

        print(f"\nFinal Reconstructed Document State: {reconstructed_doc}")
    finally:
        await reopened.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Local Event Sourcing

1. **Zero External Database Overhead**: No PostgreSQL, No Docker, No daemon processes to package or ship with your desktop installer.
2. **Instant Replay Rebuilds**: Corrupted in-memory cache? Drop memory and replay the ledger from `replay_from=0`. The app recalculates the exact truth state in milliseconds.
3. **Deterministic Simulation & Bug Triage**: When a user encounters an issue, they send their `data/desktop_events` folder. Support engineers replay the exact stream to reproduce and inspect the bug deterministically.
