# Guide 12: LLM Speculative Decoding & Verification Engines

## Problem & Architectural Challenge

In high-throughput Large Language Model serving (e.g. vLLM, SGLang, TensorRT-LLM, custom model servers), autoregressive token generation is memory-bandwidth bound.

**Speculative Decoding** accelerates inference by pairing two models:
1. **Draft Model (Small & Fast)**: A lightweight 1B–3B parameter model rapidly generates a candidate tree of $K$ future tokens (e.g. 5–8 tokens).
2. **Target Model (Large & Accurate)**: A 70B+ parameter model verifies all $K$ tokens in parallel in a single forward pass using tree attention.

### The Coordination Bottleneck
- The draft generator and target verifier must coordinate on sub-millisecond timelines.
- Accepted tokens must be immediately forwarded to the user's WebSocket stream.
- Rejected tokens must trigger an immediate rollback of the draft model's KV cache.
- If the target verifier's GPU kernel is running full forward passes, the draft model must be backpressured to prevent generating stale speculative trees.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 Fast Draft Generator (1B Model)             │
│            (Generates speculative tree of K tokens)         │
└──────────────────────────────┬──────────────────────────────┘
                               │ publish("spec/session_1/draft", candidate_tokens)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Sub-millisecond latency   │
                │  - Inflight parking limits   │
                │  - Zero-copy tensor passing  │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┴─────────────────────┐
         ▼                                           ▼
┌─────────────────┐                         ┌─────────────────┐
│ Target Verifier │                         │ Stream Monitor  │
│ (70B Model)     │                         │ (Draft Preview) │
└────────┬────────┘                         └─────────────────┘
         │
         ▼ publish("spec/session_1/verified", accepted_tokens)
┌─────────────────┐
│ Client UI / WS  │
│ (Final Output)  │
└─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def draft_model_generator(bus: Bus, session_id: str) -> None:
    """Proposes candidate token sequences at high speed."""
    for step in range(10):
        speculative_candidate = {
            "step": step,
            "draft_tokens": ["The", " capital", " of", " France", " is"],
            "probabilities": [0.95, 0.98, 0.99, 0.92, 0.96],
        }
        await bus.publish(
            f"spec/{session_id}/draft", json.dumps(speculative_candidate).encode("utf-8")
        )
        await asyncio.sleep(0.01)  # 10ms draft latency


async def target_model_verifier(bus: Bus, session_id: str) -> None:
    """Verifies candidate token tree in one forward pass."""
    sub = await bus.subscribe(
        f"spec/{session_id}/draft",
        inflight_limit=2,  # Parks draft generator if verifier falls behind
        queue_size=4,
    )
    async for envelope in sub:
        candidate = json.loads(envelope.payload.decode())
        # Simulate target forward pass verifying 4 out of 5 tokens:
        await asyncio.sleep(0.03)  # 30ms forward pass
        accepted = candidate["draft_tokens"][:4] + ["Paris"]

        # Publish final verified tokens to user stream:
        await bus.publish(f"spec/{session_id}/verified", json.dumps(accepted).encode("utf-8"))
        await sub.ack(envelope)


async def client_streamer(bus: Bus, session_id: str) -> None:
    """Streams only verified tokens to the final client."""
    sub = await bus.subscribe(f"spec/{session_id}/verified")
    async for envelope in sub:
        tokens = json.loads(envelope.payload.decode())
        print(f"[Verified Stream] {' '.join(tokens)}")


async def main() -> None:
    async with await Bus.open(
        "data/spec_bus",
        overflow=OverflowPolicy.BLOCK,
        tail_cache_bytes=32 * 1024 * 1024,
    ) as bus:
        session = "query_42"
        await asyncio.gather(
            draft_model_generator(bus, session),
            target_model_verifier(bus, session),
            client_streamer(bus, session),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Speculative Decoding

1. **In-Process Tail Cache Latency**: Passing candidate tokens and logit references happens in RAM in sub-milliseconds without cross-process IPC socket overhead.
2. **Inflight Bounded Pipelining**: `inflight_limit=2` strictly caps speculative tree generation so the draft model never proposes tokens based on stale KV cache state.
