Status: done
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md

# P3 A12: byte-first segments and shared Arrow allocation accounting

## Scope

Replace canonical segmentation v1 with the measured byte-first v2 policy, remove missing-policy compatibility, count shared Arrow allocations once at managed-memory admission, and expose phase telemetry on ordinary CLI runs so package regressions are attributable.

## Acceptance criteria

- Microbatch and canonical segment bounds are independent and plan-versioned.
- January TLC produces materially fewer durable segments and lower package wall time without changing rows, order, schema, provenance, receipt, or checkpoint semantics.
- Shared backing buffers are counted once and zero-copy IPC cannot trigger a false oversized-window error.
- Ordinary CLI runs persist bounded phase measurements without affecting package identity.
- No v1 constructor, namespace, or missing-policy deserialization default remains.

## Evidence expectations

Focused engine/memory/package/CLI tests, live and local TLC before/after phase timings, LZ4-versus-uncompressed comparison, strict lint, secret scan, and adversarial review.

## Explicit exclusions

Parallel segment encoding, relaxed durability, changing SHA-256, changing canonical LZ4 IPC, destination-window redesign, and remote download/decode overlap.

## Blockers

None.

## References

- `.10x/decisions/byte-first-canonical-segmentation-v2.md`
- `.10x/specs/canonical-segmentation-adaptive-batching.md`
- `.10x/specs/package-io-hashing-durability.md`

## Progress and notes

- 2026-07-11: Measured 46 2.38-MiB encoded TLC segments under v1. Implemented v2 at a 32-MiB logical target/64-MiB hard maximum, retained the independent 8K–64K microbatch bounds, removed missing-policy compatibility, enabled bounded CLI phase metrics, and added shared-allocation-aware Arrow retained-memory accounting.
- 2026-07-11: Closure verification passed all active engine/package/memory/source-files tests, focused format/HTTP integration, and strict lint. Review removed the source-owned Parquet branch in favor of one format-driver stream entry point. V2 reduced TLC durable publish 363.1→141.8 ms and package execution 1,617.4→1,363.2 ms. Evidence: `.10x/evidence/2026-07-11-p3-parquet-stream-byte-first-segments.md`. Review: `.10x/reviews/2026-07-11-p3-parquet-stream-byte-first-review.md`.
