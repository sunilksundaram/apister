# Guide 02: RAG Ingestion & Multi-Index Embedding Fan-Out

## Problem & Architectural Challenge

In high-scale Retrieval-Augmented Generation (RAG) pipelines, ingesting thousands of PDF documents, web pages, or database records requires chunking and generating dense vector embeddings (e.g. OpenAI `text-embedding-3`, BGE, Nomic).

Each generated embedding (often a 1536- or 4096-dimensional `float32` vector, ~6–16 KB per record) must be fanned out to multiple destinations:
1. **Vector Database**: For dense approximate nearest neighbor (ANN) search (Qdrant, Milvus, Pinecone).
2. **Lexical Index**: For keyword / BM25 hybrid search (Elasticsearch / Tantivy).
3. **Semantic Cache**: Stored in Redis to quickly short-circuit identical future user queries.
4. **Knowledge Graph Ingestion**: Extracting semantic triples for GraphRAG.

### Why Standard Queues & External Brokers Fail Here
- **Memory Amplification**: In Python, broadcasting 100,000 vectors across 3 queues duplicates hundreds of megabytes of raw float matrices in RAM, causing heavy garbage collection latency.
- **Expensive Re-computation on Crash**: Embedding API calls and local GPU inference are expensive. If the vector database connection drops at chunk 85,000, standard in-memory pipelines crash and lose progress. Reprocessing 85,000 chunks wastes thousands of dollars and hours of GPU time.

---

## Architectural Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    Document Chunking & Embedder              │
│       (Local SentenceTransformers or Remote Embedding API)   │
└──────────────────────────────┬───────────────────────────────┘
                               │ publish("rag/chunk/doc_42/part_1", embedding_payload)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Zero-copy float storage   │
                │  - Segmented on-disk log     │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Vector Database │   │ Lexical / BM25  │   │  Semantic Cache │
│ (ANN Indexing)  │   │ (Hybrid Search) │   │ (Redis Store)   │
│ Cursor: "vdb"   │   │ Cursor: "bm25"  │   │ Cursor: "cache" │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
import struct
from pathlib import Path
from apister import Bus, OverflowPolicy


def pack_embedding_chunk(chunk_id: str, text: str, vector: list[float]) -> bytes:
    """Serializes text metadata alongside packed binary float32 embeddings."""
    metadata = json.dumps({"id": chunk_id, "text": text}).encode("utf-8")
    floats_binary = struct.pack(f"{len(vector)}f", *vector)
    # Binary layout: [4 bytes metadata len] + [metadata JSON] + [raw floats]
    return struct.pack("<I", len(metadata)) + metadata + floats_binary


def unpack_embedding_chunk(payload: bytes) -> tuple[dict, list[float]]:
    meta_len = struct.unpack("<I", payload[:4])[0]
    metadata = json.loads(payload[4 : 4 + meta_len].decode("utf-8"))
    floats_bytes = payload[4 + meta_len :]
    count = len(floats_bytes) // 4
    vector = list(struct.unpack(f"{count}f", floats_bytes))
    return metadata, vector


async def vector_db_indexer(bus: Bus) -> None:
    """Worker ingesting vectors into Qdrant/Milvus with persistent resume cursor."""
    sub = await bus.subscribe("rag/chunk/#", name="indexer-vector-db", inflight_limit=32)
    async for envelope in sub:
        # Zero-copy buffer protocol avoids copying heavy embedding payloads
        metadata, vector = unpack_embedding_chunk(envelope.payload)
        # Mocking async insert into Vector DB:
        await asyncio.sleep(0.01)
        # Acknowledge completion - advances on-disk cursor
        await sub.ack(envelope)


async def lexical_bm25_indexer(bus: Bus) -> None:
    """Worker indexing the raw text into BM25 for hybrid search."""
    sub = await bus.subscribe("rag/chunk/#", name="indexer-bm25", inflight_limit=64)
    async for envelope in sub:
        metadata, _vector = unpack_embedding_chunk(envelope.payload)
        # Mocking async insert into Elasticsearch / BM25:
        await asyncio.sleep(0.005)
        await sub.ack(envelope)


async def document_producer(bus: Bus) -> None:
    """Simulates an embedding generator pipeline producing batches of chunks."""
    batch: list[tuple[str, bytes]] = []
    for i in range(100):
        # 1536-dimensional float vector
        fake_vector = [0.01 * (j % 100) for j in range(1536)]
        payload = pack_embedding_chunk(
            f"chunk_{i}", f"Document text content for chunk {i}", fake_vector
        )
        batch.append((f"rag/chunk/doc_01/{i}", payload))

    # Atomically write and fan out embedding batch in a single dispatch cycle
    await bus.publish_many(batch)


async def main() -> None:
    # Storage preserves state across process restarts with auto-selected Rust core
    async with await Bus.open(
        "data/rag_bus",
        backend="auto",  # Automatically uses Rust acceleration if available
        overflow=OverflowPolicy.BLOCK,  # Backpressure: pauses producer if indexer lags
        max_payload_bytes=16 * 1024 * 1024,
    ) as bus:
        await asyncio.gather(
            document_producer(bus),
            vector_db_indexer(bus),
            lexical_bm25_indexer(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in RAG Ingestion

1. **Monotonic Recovery Without Re-Embedding**: If your machine restarts halfway through indexing 1,000,000 chunks, `indexer-vector-db` re-subscribes with `name="indexer-vector-db"` and immediately resumes at chunk 500,001.
2. **Backpressure Protects GPU & RAM**: If Qdrant takes longer to index high-dimensional vectors, `inflight_limit=32` parks further deliveries, and `OverflowPolicy.BLOCK` naturally throttles the upstream embedding generator.
