# apister Production Use Case Guides

This directory contains practical, production-ready architectural guides for leveraging **`apister`** across AI/GenAI, machine learning, computer vision, robotics, finance, bioinformatics, and desktop systems.

Unlike distributed network brokers (Kafka, NATS, RabbitMQ) that coordinate services across a datacenter, `apister` is an **embedded, in-process publish-subscribe engine** designed to eliminate memory duplication, prevent event-loop stalls, and provide crash-durable messaging inside a single Python process.

---

## ⚡ High-Throughput & Zero-Copy Engine Features

Every pattern documented in this directory leverages `apister`'s core architectural advantages:
- **Hybrid Rust Core (`backend="auto" | "rust" | "python"`)**: Automatically leverages the PyO3/Maturin native engine for **1.26M+ writes/sec**, hardware SIMD CRC32, and native radix MQTT topic matching (**3.19M+ matches/sec**).
- **Batch Publishing API (`bus.publish_many()`)**: Write and fan out hundreds or thousands of telemetry samples, embeddings, or order book ticks in a single atomic dispatch turn.
- **Zero-Copy Buffer Protocol (`envelope.memoryview()` & `envelope.to_numpy()`)**: Avoid heap allocations when consuming binary payloads, audio PCM buffers, camera frames, and embedding vectors directly into NumPy, OpenCV, PyTorch, or struct unpackers.
- **Microsecond Topic Match Memoization**: Hot-topic publishes bypass Trie traversal ($O(1)$ dictionary memoization yielding up to **16M matches/sec**), auto-invalidated on subscription changes.
- **Tail Cache**: Zero disk I/O for real-time consumers—messages are served straight from memory while asynchronously persisting to segmented on-disk logs.

> 🛡️ For hardware sizing runbooks, Systemd service templates, Docker container patterns, and crash-safety audits, see the **[Enterprise Production Readiness Guide](../enterprise-production-guide.md)**.

---

## Guide Index by Domain

### 🤖 Generative AI & Large Language Models
1. **[Multi-Agent AI & Streaming LLM Orchestration](01-llm-multi-agent-orchestration.md)**
   *Fan-out real-time reasoning tokens to UI streams, live guardrail validators, cost monitors, and trajectory evaluators without memory amplification.*
2. **[RAG Knowledge Ingestion & Multi-Index Embedding Fan-Out](02-rag-multi-index-embedding-fanout.md)**
   *Distribute heavy high-dimensional embedding vectors simultaneously to Vector DBs, BM25 indices, and semantic caches with persistent, crash-proof resume cursors.*
3. **[RLHF, DPO & PPO Rollout Trajectory Buffers](03-rlhf-dpo-rollout-buffers.md)**
   *Buffer and inspect long actor rollouts, reward models, and reference policies simultaneously without copying multi-megabyte tensor sequences.*
4. **[Real-Time Multimodal Streaming (Voice, Audio & Video)](04-realtime-multimodal-streaming.md)**
   *Interleave raw PCM audio chunks, camera frames, and LLM text tokens with sub-millisecond hot-cache latency and zero dropped frames.*
5. **[Synthetic Data Generation & Backpressure Pipelines](05-synthetic-data-generation-pipelines.md)**
   *Orchestrate Self-Instruct / Evol-Instruct workflows where slow LLM-as-a-judge evaluators or execution sandboxes regulate fast generator prompts.*
6. **[ML Training Telemetry & Metric Outbox](06-ml-training-telemetry-outbox.md)**
   *Zero-latency metric and gradient logging for multi-GPU training loops; buffer telemetry safely to disk during cloud connectivity blips.*
7. **[LLM Speculative Decoding & Verification Engine](12-llm-speculative-decoding-engine.md)**
   *Coordinate fast 1B draft model token tree proposals and 70B target model forward-pass verifications with bounded inflight limits.*
8. **[Automated LLM Red-Teaming & Adversarial Jailbreak Fuzzing](13-llm-redteaming-and-adversarial-fuzzing.md)**
   *Execute multi-day adversarial mutation sweeps across target model APIs with crash-safe resume cursors and immutable audit logs.*

---

### 👁️ Edge, Robotics, Vision & Signal Processing
9. **[Computer Vision & Real-Time Perception Loops](07-computer-vision-perception-loops.md)**
   *Ingest 30/60 FPS uncompressed video frames once; deliver 8-byte offsets to YOLO, OCR, and biometric models concurrently.*
10. **[Edge & Robotics Black-Box Flight Data Recorder](08-edge-robotics-flight-recorder.md)**
    *High-frequency IMU, LiDAR, and motor telemetry storage on resource-constrained companion computers with automatic flash-storage retention limits.*
11. **[Offline-First IoT Store-and-Forward & Cloud Outbox Bridge](14-offline-first-iot-store-and-forward.md)**
    *Buffer continuous industrial telemetry on disk during network disconnects and stream to central Redis/Kafka upon reconnect with deduplication.*
12. **[Autonomous Drone & Robotics Hardware-in-the-Loop (HIL) Simulation](17-drone-hardware-in-the-loop-simulation.md)**
    *Record multi-sensor flight telemetry at 500 Hz and replay deterministically from byte offset 0 through simulated flight controller hardware.*
13. **[Multi-Camera Edge NVR & Surveillance Vision](18-multi-camera-nvr-surveillance.md)**
    *Route 1080p/4K streams from multiple cameras across motion detectors, GPU inference models, and rolling disk buffers with zero memory bloat.*
14. **[Live Audio Studio, DSP & Multi-Track Recording](19-live-audio-dsp-recording-studio.md)**
    *Stream 48kHz uncompressed PCM audio chunks into live DSP filters, 60 FPS visualizers, and lossless disk archivers without audio glitches.*

---

### 📈 Financial Markets, Blockchain & Trading
15. **[Financial Market Data Feeds & Multi-Strategy Trading](09-financial-market-data-trading.md)**
    *Ingest thousands of WebSocket ticker and order book updates per second and fan out across independent trading bots using MQTT wildcards.*
16. **[High-Frequency Crypto / DEX Liquidity Arbitrage Engine](20-dex-crypto-arbitrage-engine.md)**
    *Track decentralized liquidity pools, mempool transactions, and prices in real time with sub-millisecond RAM delivery and immutable audit tapes.*

---

### 🛠️ Systems, Data Infrastructure & Scientific Workloads
17. **[Local Event Sourcing, CQRS & Deterministic Replay](10-local-event-sourcing-and-replay.md)**
    *Build crash-resilient desktop/CLI apps with immutable domain logs, full state rebuilds from byte offset 0, and time-travel simulation testing.*
18. **[Resilient Web Scraping & Crawler Worker Pipelines](11-web-scraping-and-crawler-pipelines.md)**
    *Execute high-throughput crawling with crash-proof restart recovery, zero duplicate page downloads, and automatic proxy backpressure.*
19. **[Durable In-Process Job Queues & Worker Pools](15-durable-inprocess-worker-pools.md)**
    *Implement zero-broker background worker pools with persistent task acknowledgments, out-of-order gap tracking, and zero lost jobs on reboot.*
20. **[Embedded Search & Codebase Indexing Pipelines](16-codebase-indexing-and-search.md)**
    *Fan out local codebase documents simultaneously to SQLite FTS5, tree-sitter AST parsers, and neural embedding models with independent resume cursors.*
21. **[Database Change Data Capture (CDC) Staging & Outbox](21-database-cdc-staging-outbox.md)**
    *Buffer Postgres logical replication / MySQL binlogs to decouple primary database WAL files from slow external webhooks and search indexers.*
22. **[Digital Forensics & Live Incident Response (DFIR)](22-digital-forensics-incident-response.md)**
    *Collect volatile endpoint syscalls, process trees, and sockets with tamper-evident CRC32 magic framing (`b"AP"`) and restricted `0o700` POSIX directories.*
23. **[Bioinformatics & Genomic Sequence Streaming](23-bioinformatics-genomic-streaming.md)**
    *Buffer 48-hour continuous FASTQ reads from Oxford Nanopore sequencers, delivering live reads to adaptive sampling ejectors and durable genome aligners.*
24. **[Edge Federated Learning Gradient Buffers & Cloud Outbox](24-edge-federated-learning-buffers.md)**
    *Buffer local gradient weight updates safely on edge devices and stream to central federated coordinators with loop-free broker deduplication.*

---

## When to Choose `apister`

```
                                 Is the communication...
                                            │
                     ┌──────────────────────┴──────────────────────┐
                     ▼                                             ▼
             Between Machines                              Inside One Process
                    │                                              │
         ┌──────────┴──────────┐                        ┌──────────┴──────────┐
         ▼                     ▼                        ▼                     ▼
    Need Clustering/     Lightweight RPC           Large Payloads /      Simple Ephemeral
    Multi-DC Rebalance    or Microservices         Durable Acks /         Events (Low Volume)
         │                     │                   Zero-Copy Fanout            │
         ▼                     ▼                        │                      ▼
       Kafka                 NATS                       ▼                asyncio.Queue
                                                     apister
```
