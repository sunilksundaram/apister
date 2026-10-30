# GitHub Copilot Instructions for Apister

When writing code or assisting with the `apister` codebase or applications using `apister`, adhere to the instructions and patterns documented in the agent skills:

- **Core Pub/Sub Architecture**: Refer to [`.agent/skills/apister-pubsub/SKILL.md`](../.agent/skills/apister-pubsub/SKILL.md) for zero-copy fan-out, persistent ack cursors, inflight message parking, and backpressure policies.
- **Broker Bridging**: Refer to [`.agent/skills/apister-broker-bridging/SKILL.md`](../.agent/skills/apister-broker-bridging/SKILL.md) for Redis, RabbitMQ, and Kafka adapters, reconnection policies, and deduplication.

## Critical Invariants
1. **Never copy payloads across queues**: Subscriber queues hold integer byte offsets. Payloads are read through `_materialize(offset)` from the tail cache or ledger.
2. **Monotonic cursors**: Out-of-order `sub.ack()` only advances the cursor across contiguous acknowledged prefixes.
3. **Loop suppression**: External broker bridges must respect 16-byte `instance_id` framing to prevent echoing messages.
4. **Clean shutdown**: Always handle `SubscriptionClosed` when consuming via `async for` or `sub.get()`.

