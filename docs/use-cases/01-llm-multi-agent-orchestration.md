# Guide 01: Multi-Agent AI & Streaming LLM Orchestration

## Problem & Architectural Challenge

In complex agentic architectures (e.g. multi-agent research swarms, autonomous coding assistants, and interactive tool-calling loops), a central model generates high-velocity streaming tokens, reasoning thoughts, and tool invocation payloads.

Multiple in-process components must react to this exact same stream simultaneously:
1. **WebSocket Streamer**: Sends tokens to the user interface in real time.
2. **Safety & Guardrail Validator**: Scans reasoning tokens for toxic outputs, jailbreaks, or PII leakage.
3. **Token & Cost Meter**: Aggregates token counts and halts execution if the user's budget is exceeded.
4. **Trajectory & Evaluation Logger**: Persists the full thought chain for offline fine-tuning or model benchmarking.

Using standard `asyncio.Queue` duplicates every token chunk across 4 separate queues. If the agent generates thousands of tokens or large tool payloads (e.g., file diffs, web page extracts), memory balloons and GC pauses cause jitter in the live UI stream.

---

## Architectural Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    LLM Streaming Generator                   │
│   (OpenAI / Anthropic / Local vLLM Inference Coroutine)       │
└──────────────────────────────┬───────────────────────────────┘
                               │ await bus.publish("agent/session1/tokens", chunk)
                               ▼
                ┌──────────────────────────────┐
                │        apister Bus           │
                │  - Payload written once      │
                │  - 8-byte offset distributed │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┬─────────────────────┐
         ▼                     ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ WebSocket Stream│   │ Real-time Guard │   │ Token/Cost Meter│   │ Trajectory Log  │
│ (Live to Client)│   │ (PII & Toxicity)│   │ (Budget Alerts) │   │ (Durable Trace) │
└─────────────────┘   └─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Topic Hierarchy Design

- `agent/<session_id>/thought`: Chain-of-thought and internal reasoning tokens.
- `agent/<session_id>/output`: User-facing response tokens.
- `agent/<session_id>/tools/<tool_name>/call`: Outbound function call parameters.
- `agent/<session_id>/tools/<tool_name>/result`: Inbound tool execution returns.

Subscribers use MQTT wildcards:
- `agent/+/output`: Global UI streamer monitoring all active user sessions.
- `agent/session_42/#`: Complete session listener for auditing and evaluation.

---

## Production Implementation

```python
import asyncio
from pathlib import Path
from apister import Bus, OverflowPolicy


async def agent_streamer(bus: Bus, session_id: str) -> None:
    """Simulates an LLM streaming reasoning and output tokens."""
    tokens = ["Thinking", " through", " the", " query...", "\nHere", " is", " the", " solution."]
    for token in tokens:
        topic = (
            f"agent/{session_id}/thought" if "Thinking" in token else f"agent/{session_id}/output"
        )
        await bus.publish(topic, token.encode("utf-8"))
        await asyncio.sleep(0.02)  # Simulating 50 tokens/sec


async def websocket_client_consumer(bus: Bus, session_id: str) -> None:
    """Streams user-facing tokens directly to the client WebSocket."""
    sub = await bus.subscribe(f"agent/{session_id}/output")
    print("[UI] Connected to output stream...")
    async for envelope in sub:
        print(f"[UI Stream] {envelope.payload.decode()}", end="", flush=True)


async def guardrail_safety_monitor(bus: Bus, session_id: str) -> None:
    """Inspects all tokens (thoughts and outputs) for disallowed patterns."""
    sub = await bus.subscribe(f"agent/{session_id}/#", name=f"guardrail-{session_id}")
    async for envelope in sub:
        text = envelope.payload.decode()
        if "MALICIOUS" in text:
            print(f"\n[ALERT] Guardrail tripped on topic {envelope.topic}!")
        await sub.ack(envelope)


async def trajectory_eval_recorder(bus: Bus, session_id: str) -> None:
    """Durable recorder tracking the agent's full execution trace."""
    sub = await bus.subscribe(f"agent/{session_id}/#", name=f"audit-{session_id}")
    async for envelope in sub:
        # Full envelope contains timestamp_ns, offset, message_id
        # Perfect for constructing fine-tuning datasets
        await sub.ack(envelope)


async def main() -> None:
    storage = Path("data/agent_bus")
    async with await Bus.open(
        storage,
        tail_cache_bytes=16 * 1024 * 1024,  # Keep hot token streams in RAM
        overflow=OverflowPolicy.DROP_OLDEST,  # Live UI drops oldest if network stalls
    ) as bus:
        session = "sess_001"
        await asyncio.gather(
            agent_streamer(bus, session),
            websocket_client_consumer(bus, session),
            guardrail_safety_monitor(bus, session),
            trajectory_eval_recorder(bus, session),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Recommended `Bus.open()` Configuration

| Setting | Recommended Value | Reason |
| :--- | :--- | :--- |
| `tail_cache_bytes` | `16–64 MiB` | Holds all active streaming tokens in RAM; consumers never hit disk. |
| `overflow` | `OverflowPolicy.DROP_OLDEST` | Fast token generators won't stall if a client's WebSocket slows down. |
| `commit_every` | `256` | Batches disk commits to keep the training/trajectory ledger efficient. |
