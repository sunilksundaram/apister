# Guide 16: Embedded Search & Codebase Indexing Pipelines

## Problem & Architectural Challenge

Desktop IDEs, code editors, and local search tools (e.g., Obsidian plugins, local code search utilities, desktop file search tools) must index large software repositories or document trees into multiple specialized engines concurrently:
- **SQLite FTS5**: For lightning-fast exact keyword and substring search.
- **Tree-sitter / AST Parser**: For language symbols, references, and semantic navigation graphs.
- **Vector Embedding Store**: For semantic, natural-language search over code docstrings and functions.

### The Pitfalls
1. **Memory Amplification**: Ingesting hundreds of source files into distinct `asyncio.Queue` pipelines creates duplicate copies of every file string in memory.
2. **Re-Indexing from Scratch on Crash**: If the user closes their laptop, quits the IDE, or an unhandled parsing exception occurs, in-memory queues lose all state. The application is forced to re-index the entire codebase from scratch on next launch.

### The Solution: Zero-Copy Multi-Indexer with Persistent Cursors
`apister` allows the file reader to publish raw file contents once into an on-disk segmented ledger. Multiple independent indexer coroutines subscribe and consume at their own speeds, advancing durable on-disk cursors.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    Filesystem Crawler Task                  │
│             (Scans local files, emits raw text)             │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish("index/file", payload)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Zero-copy payload access  │
                │  - Segmented disk ledger     │
                │  - Independent named cursors │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ SQLite FTS5     │   │ AST Tree-Sitter │   │ Embedding Model │
│ Inverted Index  │   │ Symbol Parser   │   │ Vector Store    │
│ (Cursor: "fts") │   │ (Cursor: "ast") │   │ (Cursor: "vec") │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def file_system_crawler(bus: Bus) -> None:
    """Simulates reading source files from a git repository."""
    for i in range(20):
        code_document = {
            "path": f"src/module_{i}.py",
            "content": f"def function_{i}():\n    return 'result_{i}'\n\nclass Model_{i}:\n    pass",
        }
        await bus.publish("index/source_file", json.dumps(code_document).encode("utf-8"))
        await asyncio.sleep(0.01)


async def sqlite_fts_indexer(bus: Bus) -> None:
    """Builds tokenized full-text search index."""
    sub = await bus.subscribe("index/source_file", name="fts_index_worker", inflight_limit=32)
    async for envelope in sub:
        doc = json.loads(envelope.payload.decode())
        # Simulated SQLite FTS5 INSERT OR REPLACE:
        # await db.execute("INSERT INTO code_fts VALUES (?, ?)", (doc["path"], doc["content"]))
        print(f"  [FTS Indexer] Tokenized keywords for {doc['path']}")
        await sub.ack(envelope)


async def tree_sitter_ast_indexer(bus: Bus) -> None:
    """Parses syntax trees to extract symbols and call hierarchies."""
    sub = await bus.subscribe("index/source_file", name="ast_tree_worker", inflight_limit=16)
    async for envelope in sub:
        doc = json.loads(envelope.payload.decode())
        # Simulated AST symbol extraction:
        print(f"  [AST Parser] Extracted function & class definitions for {doc['path']}")
        await sub.ack(envelope)


async def vector_embedding_indexer(bus: Bus) -> None:
    """Slow worker: computes dense neural embeddings for semantic search."""
    sub = await bus.subscribe("index/source_file", name="vector_worker", inflight_limit=8)
    async for envelope in sub:
        doc = json.loads(envelope.payload.decode())
        # Simulating heavy neural embedding calculation (e.g. 50ms per file):
        await asyncio.sleep(0.03)
        print(f"  [Vector Model] Embedded docstrings for {doc['path']}")
        await sub.ack(envelope)


async def main() -> None:
    storage = Path("data/index_ledger")
    async with await Bus.open(
        storage,
        segment_bytes=16 * 1024 * 1024,
        retain_bytes=500 * 1024 * 1024,
        overflow=OverflowPolicy.BLOCK,
    ) as bus:
        await asyncio.gather(
            file_system_crawler(bus),
            sqlite_fts_indexer(bus),
            tree_sitter_ast_indexer(bus),
            vector_embedding_indexer(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Best Practices & Tuning

1. **Named Persistent Cursors**: Using `name="fts_index_worker"` and `name="vector_worker"` ensures each subscriber tracks its own position in on-disk cursor files. If an exception hits the vector model, the FTS indexer is unaffected.
2. **Backpressure with `OverflowPolicy.BLOCK`**: When embedding models run significantly slower than disk I/O, `BLOCK` automatically throttles the file crawler without dropping source files.
3. **Zero Copy**: Large source files (or bundled minified JS files) reside in memory once; all indexers receive non-destructive slices through `Envelope.payload`.

