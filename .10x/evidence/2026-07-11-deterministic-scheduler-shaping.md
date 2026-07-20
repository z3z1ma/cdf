Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/canonical-frontier-parallel-scheduling.md, .10x/specs/deterministic-parallel-scheduler.md, .10x/tickets/done/2026-07-10-p3-ws-c-deterministic-parallelism.md

# Deterministic scheduler shaping evidence

## What was observed

Scan partitions are ordered but lack explicit ordinal/admission/retry/speculation authority; execution is sequential with mutable global limit; REST rate/retry is driver-local; file subunits must not independently advance manifest state; nested libraries can oversubscribe.

## Procedure

Traced plan/partition/capability structures, sequential engine limit/order, source retry/rate behavior, file state assembly, and active host/memory/format/source contracts.

## What this supports

Hierarchical global admission, canonical commit frontier, bounded reorder/lookahead, canonical limits, retry/attestation, scope leases, and shared native CPU slots.

## Limits

This is shaping evidence. C1-C4 must calibrate and prove utilization, determinism, bounded memory, and framework overhead.
