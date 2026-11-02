# apister_core

Native Rust accelerator for [`apister`](https://github.com/sunilksundaram/apister), the zero-copy asyncio publish-subscribe bus with durable on-disk ledger storage.

## Features

- **Hardware SIMD CRC32**: Hardware-accelerated IEEE 802.3 CRC32 checksums via `crc32fast`.
- **Segmented Native Ledger**: High-throughput append-only disk storage engine (~1.26M+ writes/sec) with memory-mapped slice reads (`memmap2`).
- **Radix Topic Trie**: High-performance MQTT wildcard topic matching (~3.19M+ matches/sec).
- **Zstandard Compression & Dictionary Training**: Native Zstd compression/decompression and statistical dictionary training for micro-payloads via `zstd`.
- **Streaming LZ4 Frame Compression**: Ultra-low latency frame compression via `lz4_flex`.
- **Zero-Allocation W3C Tracing**: In-band `traceparent` context injection and extraction.

## Usage

This crate is typically consumed transparently by Python via PyO3 when installing `apister`:

```bash
pip install apister
```

Or built locally via `maturin`:

```bash
maturin develop --release
```

## License

MIT License. See [LICENSE](https://github.com/sunilksundaram/apister/blob/main/LICENSE) for details.

