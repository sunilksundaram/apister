# Guide 20: High-Frequency Crypto / DEX Liquidity Arbitrage Engine

## Problem & Architectural Challenge

Decentralized finance (DeFi) and automated market maker (AMM) arbitrage bots monitor state transitions across dozens of liquidity pools (e.g. Uniswap v3, Curve, Balancer, Sushiswap):
- WebSocket nodes emit hundreds of mempool pending transactions, block headers, and pool swap events per second.
- Incoming events must be routed immediately across multiple specialized trading subroutines:
  1. **MEV / Sandwich Detector**: Flags large pending swaps in the mempool.
  2. **Cross-DEX Triangular Arbitrage Calculator**: Evaluates triangular pricing spreads (e.g., USDC -> WETH -> DAI -> USDC).
  3. **Gas Price Predictor**: Dynamically estimates base fees and priority tips.
  4. **Transaction Signer & Flashbots Submitter**: Signs and broadcasts bundles when profit exceeds gas thresholds.

### The Pitfalls
- **Routing Overhead & Fan-Out Lag**: In high-frequency trading, a 5-millisecond delay means another bot claims the arbitrage opportunity.
- **Lost Audit Trail**: If a trade loses money due to slippage, analyzing the exact sequence of historical market events requires an immutable, zero-loss ledger.

### The Solution: Sub-Millisecond In-Process Fan-Out & Hierarchical Topics
`apister` delivers ticks to all strategy coroutines within microseconds. MQTT hierarchical wildcards (`dex/ethereum/+/swaps`) route pool-specific events cleanly, and the segmented ledger records an unalterable tick-by-tick tape.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 WebSocket RPC Node Listener                 │
│              (Ingests pending mempool & swap events)        │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish(topic, tick_bytes)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Sub-millisecond fan-out   │
                │  - MQTT topic patterns       │
                │  - Immutable execution tape  │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Triangular Arb  │   │ Sandwich/MEV    │   │ Flashbots Bundle│
│ Calculator      │   │ Detector        │   │ Submitter       │
│ (Sub: "dex/+/   │   │ (Sub: "mempool/ │   │ (Sub: "orders/  │
│   +/swaps")     │   │   pending/+")   │   │   executable")  │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def rpc_node_mempool_stream(bus: Bus) -> None:
    """Simulates receiving live swap and mempool events from an Ethereum node."""
    pools = ["uniswap_v3", "sushiswap", "curve"]
    for i in range(20):
        pool = pools[i % len(pools)]
        tick = {
            "block": 19_400_000 + (i // 5),
            "pool": pool,
            "token_in": "WETH",
            "token_out": "USDC",
            "amount_in": 10.0 + (i * 0.5),
            "price": 3200.0 + (i * 1.5),
        }
        topic = f"dex/ethereum/{pool}/swaps"
        await bus.publish(topic, json.dumps(tick).encode("utf-8"))
        await asyncio.sleep(0.01)


async def triangular_arbitrage_bot(bus: Bus) -> None:
    """Subscribes to all Ethereum DEX swaps using wildcard `dex/ethereum/+/swaps`."""
    sub = await bus.subscribe("dex/ethereum/+/swaps")
    processed = 0
    async for envelope in sub:
        tick = json.loads(envelope.payload.decode())
        processed += 1
        # Arbitrage spread calculation:
        spread = 0.0035 if "uniswap" in tick["pool"] else 0.0010
        if spread > 0.0025:
            # Emit executable order:
            order = {"action": "BUY_SELL_CYCLE", "spread": spread, "pool": tick["pool"]}
            await bus.publish("orders/executable", json.dumps(order).encode("utf-8"))
            print(f"  [Arb Strategy] Found {spread * 100:.2f}% opportunity in {tick['pool']}!")
        if processed >= 20:
            break


async def flashbots_order_executor(bus: Bus) -> None:
    """Executes high-value arbitrage orders via Flashbots private relay."""
    sub = await bus.subscribe("orders/executable", name="executor_cursor")
    async for envelope in sub:
        order = json.loads(envelope.payload.decode())
        print(f"    >>> [Executor] Signed & dispatched bundle for {order['pool']} <<<")
        await sub.ack(envelope)


async def main() -> None:
    storage = Path("data/crypto_hft_ledger")
    async with await Bus.open(
        storage,
        segment_bytes=16 * 1024 * 1024,
        retain_bytes=500 * 1024 * 1024,
        overflow=OverflowPolicy.DROP_OLDEST,
        tail_cache_records=512,
    ) as bus:
        await asyncio.gather(
            rpc_node_mempool_stream(bus),
            triangular_arbitrage_bot(bus),
            flashbots_order_executor(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Best Practices & Tuning

1. **`tail_cache_records=512`**: Arbitrage bots must execute on the latest market prices; a large tail-cache ensures orders are placed with sub-millisecond RAM read performance.
2. **Wildcard Partitioning**: Organizing topics by network and protocol (`dex/{network}/{exchange}/{event_type}`) allows running multiple bot algorithms in the same process with isolated concern.
3. **Audit Tape**: In the event of a trade loss, replaying the tape from offset 0 exposes exact market conditions and order timestamps.
