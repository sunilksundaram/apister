# Guide 23: Bioinformatics & Genomic Sequence Streaming

## Problem & Architectural Challenge

Next-Generation Sequencing (NGS) and third-generation sequencers (e.g. Oxford Nanopore MinION / PromethION, Pacific Biosciences Revio) generate continuous real-time streams of raw basecalls and FASTQ data over 24 to 72 hours:
- Hundreds of megabytes of raw DNA/RNA sequences per minute.
- Sequence reads must be fanned out across multiple scientific analysis pipelines:
  1. **Real-Time Quality Filtering**: Calculates Phred quality scores and trims low-confidence adapter sequences.
  2. **Reference Genome Alignment**: Streams sequences directly into alignment tools (`minimap2`, `BWA-MEM`).
  3. **Adaptive Sampling Decision Engine**: Analyzes the first 200 base pairs of a live read; if uninteresting, sends a voltage reverse command to the sequencer flowcell to physically eject the DNA molecule and free the pore for another strand.
  4. **Archival Staging**: Bundles verified reads into compressed FASTQ.GZ files for long-term clinical storage.

### The Pitfalls
- **High-Risk Runs**: A single sequencing run can cost thousands of dollars in flowcells and reagents. If an in-memory alignment script runs out of memory (OOM), in-flight reads that were not written to disk are permanently lost.
- **Processing Speed Differences**: Quality filtering takes 1 ms per read, whereas genome alignment may take 50 ms. Fast tasks must not be held back by alignment workloads.

### The Solution: Zero-Loss Persistent Cursors & Asymmetric Workloads
`apister` buffers raw FASTQ reads directly to an on-disk segmented ledger. Fast adaptive sampling algorithms read instantly from the tail-cache to make pore-ejection decisions, while slow alignment workers advance persistent cursors safely without losing data during system reboots or OOM crashes.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 Nanopore Flowcell Basecaller                │
│                 (Emits real-time FASTQ DNA reads)           │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish("dna/reads/run01", fastq)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - Zero data loss on OOM     │
                │  - Microsecond tail cache    │
                │  - Durable resume cursors    │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Ultra-Fast:     │   │ Fast: Phred QC  │   │ Slow: Genome    │
│ Adaptive Sample │   │ & Trimmer       │   │ Alignment       │
│ Decision Engine │   │ (Sub: "dna/#")  │   │ (Sub: "dna/#")  │
│ [Pore Ejection] │   │ [Persistent]    │   │ [Durable Ack]   │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def nanopore_basecaller_stream(bus: Bus) -> None:
    """Simulates real-time DNA basecaller outputting FASTQ reads."""
    bases = ["ACGT", "TGCA", "GGAT", "CCAT"]
    for read_id in range(20):
        seq = (bases[read_id % 4] * 25) + "AAAA"  # 104 bp synthetic sequence
        read_record = {
            "read_id": f"read_{read_id:04d}",
            "sequence": seq,
            "quality": "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65"[: len(seq)],
            "flowcell_channel": (read_id % 512) + 1,
        }
        await bus.publish("dna/flowcell/channel_data", json.dumps(read_record).encode("utf-8"))
        await asyncio.sleep(0.01)


async def adaptive_sampling_decision_engine(bus: Bus) -> None:
    """Ultra-fast subscriber: decides within 10ms whether to eject molecule."""
    sub = await bus.subscribe("dna/flowcell/channel_data")  # Ephemeral tail-cache subscriber
    processed = 0
    async for envelope in sub:
        read = json.loads(envelope.payload.decode())
        processed += 1
        # Fast prefix match: eject if sequence starts with non-target prefix
        is_target_organism = read["sequence"].startswith("ACGT")
        if not is_target_organism:
            # Send physical voltage reverse command to unblock pore:
            pass
        if processed >= 20:
            break
    print(
        f"  [Adaptive Sampling] Evaluated {processed} reads; issued pore unblock commands in real time."
    )


async def reference_genome_aligner(bus: Bus) -> None:
    """Slow worker: maps sequences to reference genome; advances durable on-disk cursor."""
    sub = await bus.subscribe(
        "dna/flowcell/channel_data", name="minimap2_aligner", inflight_limit=4
    )
    aligned_count = 0
    async for envelope in sub:
        read = json.loads(envelope.payload.decode())
        aligned_count += 1
        # Simulate CPU-heavy alignment algorithm:
        await asyncio.sleep(0.02)
        await sub.ack(envelope)
        if aligned_count >= 20:
            break
    print(
        f"  [Genome Aligner] Aligned {aligned_count} reads to reference genome with persistent ACK safety."
    )


async def main() -> None:
    storage = Path("data/genomic_sequencing_ledger")
    async with await Bus.open(
        storage,
        segment_bytes=16 * 1024 * 1024,
        retain_bytes=1024 * 1024 * 1024,
        overflow=OverflowPolicy.BLOCK,
    ) as bus:
        await asyncio.gather(
            nanopore_basecaller_stream(bus),
            adaptive_sampling_decision_engine(bus),
            reference_genome_aligner(bus),
        )


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Best Practices & Tuning

1. **Protect Costly Biological Samples**: Using `OverflowPolicy.BLOCK` guarantees that sequencer output is never silently discarded if the compute workstation is temporarily overloaded.
2. **Crash Resilience**: If a machine runs out of memory after 18 hours of sequencing, restarting the alignment process resumes immediately from the exact unacknowledged read offset.
3. **Zero-Copy Payload Delivery**: Passing raw FASTQ structures through `apister` avoids replicating large string allocations across multiple analyzer threads.
