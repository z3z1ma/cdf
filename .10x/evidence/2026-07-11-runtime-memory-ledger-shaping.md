Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/runtime-memory-ledger-byte-permits.md, .10x/specs/runtime-memory-backpressure.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

# Runtime memory ledger shaping evidence

## What was observed

Current runtime code has descriptive Arrow byte counts but no finite shared pool, async byte admission, spill authority, or RSS model. DataFusion's pinned tuple exposes the needed consumer/reservation/fair-spill substrate. Discovery serializes total/concurrency budgets but currently probes sequentially and never consumes those two limits.

## Procedure

Read pinned DataFusion memory-pool source and traced CDF batch, engine, discovery, dedup, package, and destination buffer paths against active runtime/memory contracts.

## What this supports

One shared DataFusion-backed coordinator, accounted envelopes, minimum-working-set admission, operator-owned escalation, weighted discovery permits, and independent RSS stress.

## Limits

This is shaping evidence. L5 must calibrate native/runtime headroom, and A2 implementation must falsify clone/cancel/deadlock/accounting behavior before the ledger is authoritative.
