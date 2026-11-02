# GitHub Copilot Instructions for Apister

When writing code or assisting with the `apister` codebase or applications using `apister`, adhere to the instructions and patterns documented in the agent skills:

- **Core Pub/Sub Architecture**: Refer to [`.agent/skills/apister-pubsub/SKILL.md`](../.agent/skills/apister-pubsub/SKILL.md) for batch publishing (`publish_many`), zero-copy buffer slicing (`to_numpy`, `memoryview`), persistent ack cursors, inflight message parking, and backpressure policies.
- **Enterprise Architecture Patterns**: Refer to [`.agent/skills/apister-architecture-patterns/SKILL.md`](../.agent/skills/apister-architecture-patterns/SKILL.md) for canonical design patterns, capacity planning formulas, and anti-patterns.
- **Broker Bridging**: Refer to [`.agent/skills/apister-broker-bridging/SKILL.md`](../.agent/skills/apister-broker-bridging/SKILL.md) for Redis, RabbitMQ, and Kafka adapters, reconnection policies, and deduplication.
- **Compression & Resilience**: Refer to [`.agent/skills/apister-compression-and-resilience/SKILL.md`](../.agent/skills/apister-compression-and-resilience/SKILL.md) for Zstandard dictionary training, zero-recompression pipelines, DLQ, W3C distributed tracing, and early disk space guarding.

## Critical Invariants
1. **Never copy payloads across queues**: Subscriber queues hold integer byte offsets. Payloads are read through `_materialize(offset)` from the tail cache or ledger.
2. **Monotonic cursors**: Out-of-order `sub.ack()` only advances the cursor across contiguous acknowledged prefixes.
3. **Loop suppression**: External broker bridges must respect 16-byte `instance_id` framing to prevent echoing messages.
4. **Clean shutdown**: Always handle `SubscriptionClosed` when consuming via `async for` or `sub.get()`.

