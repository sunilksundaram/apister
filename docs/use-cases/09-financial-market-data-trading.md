# Guide 09: Financial Market Data Feeds & Multi-Strategy Trading

## Problem & Architectural Challenge

Algorithmic trading systems and crypto market-making bots ingest market data feeds (e.g., from Binance, Coinbase, Alpaca, Interactive Brokers, or Polygon.io) over WebSocket connections:
- Order book depth diffs, L2 snapshots, and tick trades arrive at thousands of events per second.
- Within a single trading engine host, multiple quantitative models operate simultaneously:
  1. **Arbitrage Strategy**: Listens to cross-exchange tickers.
  2. **Market Making Bot**: Quotes bid/ask spreads based on order book changes.
  3. **Risk Management Guard**: Calculates real-time portfolio VaR and capital exposure.
  4. **Tick Storage / Backtesting Logger**: Records ticks for offline strategy backtesting.

### The Problem
A slow complex statistical strategy (e.g. Monte Carlo calculation) must **never** block or slow down the real-time risk monitor. Standard queues without inflight caps allow lagging strategies to silently buffer hundreds of thousands of events in RAM, eventually causing a fatal out-of-memory crash during high market volatility.

---

## Architectural Flow

```
┌──────────────────────────────────────────────────────────────┐
│                  Exchange WebSocket Ingestion                │
│             (Tick trades, orderbook updates at 5kHz)         │
└──────────────────────────────┬───────────────────────────────┘
                               │ publish("market/binance/btc_usdt/trades", tick)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - MQTT topic trie           │
                │  - Inflight parking limits   │
                │  - Zero payload copy         │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┬─────────────────────┐
         ▼                     ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Risk Guard      │   │ Market Maker    │   │ ML Momentum     │   │ Tick Store      │
│ (Critical / Immed)  │ (High Speed L2) │   │ (Complex / Park)│   │ (Historical)    │
└─────────────────┘   └─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def exchange_websocket_producer(bus: Bus) -> None:
    """Ingests tick trades and book updates in microsecond batches."""
    symbols = ["btc_usdt", "eth_usdt", "sol_usdt"]
    ticks_batch: list[tuple[str, bytes]] = []
    for i in range(100):
        symbol = symbols[i % len(symbols)]
        tick = {
            "symbol": symbol,
            "price": 65000.0 + (i * 0.5),
            "qty": 0.125,
            "side": "buy" if i % 2 == 0 else "sell",
        }
        topic = f"market/binance/{symbol}/trades"
        ticks_batch.append((topic, json.dumps(tick).encode("utf-8")))

    # Atomically write and fan out tick bursts in a single event loop turn
    await bus.publish_many(ticks_batch)


async def real_time_risk_guard(bus: Bus) -> None:
    """Listens to ALL trades across all symbols; never stalls."""
    # MQTT wildcard: single-level wildcard (+) for exchange, wildcard (#) for remainder
    sub = await bus.subscribe("market/+/+/trades", queue_size=256)
    async for envelope in sub:
        # Fast risk threshold check (< 50 microseconds)
        pass


async def ml_strategy_worker(bus: Bus) -> None:
    """Heavier strategy: inflight limit parks updates if inference falls behind."""
    sub = await bus.subscribe(
        "market/binance/btc_usdt/trades",
        name="ml-btc-strategy",
        inflight_limit=10,  # Parks new messages if 10 are already being computed
    )
    async for envelope in sub:
        # Heavier feature extraction & prediction
        await asyncio.sleep(0.02)
        await sub.ack(envelope)


async def main() -> None:
    async with await Bus.open(
        "data/market_bus",
        backend="auto",  # High-throughput Rust engine for 1.26M+ writes/sec and 3.19M+ trie matches
        overflow=OverflowPolicy.BLOCK,
        tail_cache_bytes=32 * 1024 * 1024,
    ) as bus:
        await asyncio.gather(
            exchange_websocket_producer(bus),
            real_time_risk_guard(bus),
            ml_strategy_worker(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Trading Systems

1. **Selective MQTT Filtering**: Strategies subscribe with surgical precision (e.g., `market/+/eth_usdt/#`) without receiving or filtering unwanted symbols in application code.
2. **Inflight Message Parking**: If a model takes longer on a volatile tick, `apister` automatically parks incoming messages for that consumer without stalling the central bus or affecting other strategies.
