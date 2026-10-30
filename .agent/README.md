# AI Agent Skills for Apister

This directory contains agent skills formatted according to the agent skill standard (`SKILL.md` with YAML frontmatter). These skills provide autonomous coding agents, harnesses (e.g. Claude Code, Hermes, GitHub Copilot, Cursor, OpenAI Codex/Custom Agents), and developers with structured, production-grade instructions for building and operating systems using `apister`.

---

## Available Skills

| Skill | Directory | Purpose |
| :--- | :--- | :--- |
| **`apister-pubsub`** | [`.agent/skills/apister-pubsub`](./skills/apister-pubsub/SKILL.md) | In-process zero-copy pub/sub, segmented ledger storage, durable named subscriber cursors, monotonic acknowledgment reconciliation, backpressure policies, and inflight flow control. |
| **`apister-broker-bridging`** | [`.agent/skills/apister-broker-bridging`](./skills/apister-broker-bridging/SKILL.md) | Connecting external brokers (Redis, RabbitMQ, Kafka) with automated exponential backoff, jitter, topic mapping (`/` vs `.`), loop prevention, and on-disk deduplication. |

---

## Structure

```
.agent/
├── README.md
└── skills/
    ├── apister-pubsub/
    │   ├── SKILL.md
    │   └── examples/
    │       └── worker_pool.py
    └── apister-broker-bridging/
        ├── SKILL.md
        └── examples/
            └── redis_bridge_demo.py
```

---

## Integration with Agent Harnesses

- **Claude / Anthropic Agents**: Reference `.agent/skills/apister-pubsub/SKILL.md` or `.agent/skills/apister-broker-bridging/SKILL.md` directly into the agent context or system prompt.
- **Cursor / GitHub Copilot**: Rules and contextual references can be linked directly from `.cursorrules` or `.github/copilot-instructions.md`.
- **Hermes / Custom Agent Frameworks**: Point the agent skill loader to `.agent/skills/*/SKILL.md` for automatic skill indexing.

