# Guide 11: Resilient Web Scraping & Crawler Worker Pipelines

## Problem & Architectural Challenge

High-scale asynchronous web crawlers and scraping pipelines (using Playwright, Crawlee, `aiohttp`, or `httpx`) process thousands of websites simultaneously:
- **Large Binary Payloads**: Raw HTML pages, PDF documents, and uncompressed images are downloaded concurrently.
- **Multi-Stage Processing**: Downloaded documents must be fanned out across:
  1. *Link Extractor*: Identifies new URLs to crawl and queues them.
  2. *HTML/DOM Content Parser*: Extracts structured text, JSON-LD, and metadata.
  3. *Media Asset Processor*: Resizes and hashes downloaded images.
  4. *Database Persister*: Stores clean structured records into PostgreSQL/Mongo.

### The Problem
Scrapers frequently hit target site rate limits, CAPTCHAs, or unexpected DOM exceptions that crash worker processes. Without durable local acknowledgment tracking:
- Any in-flight URLs and downloaded pages stored in memory queues are lost upon a crash.
- Restarting the scraper results in duplicate HTTP requests, burning proxy bandwidth and triggering IP bans from target domains.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 Async Web Scraper Downloaders               │
│               (Playwright / aiohttp / httpx)                │
└──────────────────────────────┬──────────────────────────────┘
                               │ publish("crawl/raw/domain_com/page_1", html_bytes)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Durable on-disk segments  │
                │  - Named ack cursors         │
                │  - Backpressure regulation   │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ DOM HTML Parser │   │ Link Extractor  │   │ Asset Downloader│
│ Cursor: "parser"│   │ Cursor: "links" │   │ Cursor: "media" │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def scraper_downloader(bus: Bus) -> None:
    """Simulates fetching HTML documents from websites."""
    for page_id in range(50):
        raw_html = f"<html><body><h1>Article {page_id}</h1><a href='/page/{page_id + 1}'>Next</a></body></html>"
        payload = json.dumps(
            {"url": f"https://example.com/page/{page_id}", "html": raw_html}
        ).encode("utf-8")
        # Under OverflowPolicy.BLOCK, downloader pauses if parsers lag behind
        await bus.publish("crawl/raw/example_com", payload)
        await asyncio.sleep(0.02)


async def html_content_parser(bus: Bus) -> None:
    """Extracts structured text with crash-proof restart recovery."""
    sub = await bus.subscribe("crawl/raw/+", name="html-content-parser", inflight_limit=8)
    async for envelope in sub:
        doc = json.loads(envelope.payload.decode())
        # Parse DOM simulation
        await asyncio.sleep(0.03)
        # Advance durable ack cursor on disk
        await sub.ack(envelope)


async def link_discovery_worker(bus: Bus) -> None:
    """Extracts outbound links for frontier crawling."""
    sub = await bus.subscribe("crawl/raw/+", name="link-discovery", inflight_limit=16)
    async for envelope in sub:
        doc = json.loads(envelope.payload.decode())
        # Discover and queue links
        await sub.ack(envelope)


async def main() -> None:
    async with await Bus.open(
        "data/crawler_bus",
        overflow=OverflowPolicy.BLOCK,  # Protects proxy bandwidth and RAM
        commit_every=128,
        segment_bytes=32 * 1024 * 1024,
    ) as bus:
        await asyncio.gather(
            scraper_downloader(bus),
            html_content_parser(bus),
            link_discovery_worker(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Why `apister` Excels in Web Crawling

1. **Zero Duplicate Crawls**: Named cursors track contiguous acknowledged pages. If the process is killed by an OOM or machine reboot, the crawler resumes on restart with zero duplicate HTTP requests.
2. **Backpressure Throttles Proxy Bandwidth**: If downstream parsing or database storage slows down, `OverflowPolicy.BLOCK` naturally pauses new page downloads.
