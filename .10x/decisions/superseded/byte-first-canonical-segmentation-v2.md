Status: superseded
Created: 2026-07-11
Updated: 2026-07-21

# Byte-first canonical segmentation v2

## Context

The v1 policy used the 65,536-row execution microbatch ceiling as the canonical segment hard ceiling. On the January 2024 TLC Parquet file this produced 46 segments averaging 2.38 MiB encoded, even though the plan declared an 8 MiB byte target. Each segment separately created an IPC encoder, synced its file, renamed it, synced its directory, and entered package evidence. Measured release telemetry attributed 359 ms to encode/hash and 363 ms to durable publication. The policy coupled an internal decode scheduling unit to durable package identity and under-amortized fixed work.

The same investigation showed that `RecordBatch::get_array_memory_size` is not retained-memory authority: Arrow documents that it can count shared backing buffers once per column. An uncompressed IPC experiment reported a 28 MiB segment as retaining 667 MiB because nineteen zero-copy columns shared one allocation. Zero-copy IPC, mmap, FFI, and sliced arrays require allocation-aware accounting.

CDF has no deployed legacy artifacts. Preserving a missing-policy deserialization shim or the v1 namespace would add compatibility code without a customer.

## Decision

Canonical segmentation v2 separates execution microbatches from durable segments:

- execution microbatches remain adaptive within 8,192–65,536 rows and 1–32 MiB;
- canonical segments are byte-first with a 32 MiB logical target and 64 MiB logical hard maximum;
- 1,048,576 target rows and 4,194,304 maximum rows are safety backstops for extremely narrow inputs, not decode-batch targets;
- segment identity uses `partition-segment-ordinal-v2`, remains partition-local, deterministic, position-authoritative, and independent of scheduling;
- new plans MUST contain the complete v2 policy; missing or unsupported policy versions fail deserialization/validation, with no v1 default or artifact compatibility shim;
- managed retained-memory accounting counts each live Arrow backing allocation once, plus container overhead, even when arrays, slices, nested children, or IPC columns share it.

The existing LZ4 Arrow IPC artifact format remains unchanged. A controlled uncompressed run reduced package execution but increased bytes from 109 MiB to 437 MiB and did not improve end-to-end local execution, so the evidence does not justify changing D-4.

## Alternatives considered

- Keep 64K canonical rows and parallelize 46 tiny writes: rejected because parallelism would hide a malformed durable unit and amplify metadata/fsync pressure at scale.
- Make canonical boundaries adapt to live pressure: rejected because package identity would depend on scheduling.
- Use 64 MiB target segments now: rejected because a TLC segment retained about 72 MiB and exceeded the current verified destination window; v2 keeps headroom while later graph admission may tune declared windows coherently.
- Trust `get_array_memory_size`: rejected because the Arrow API explicitly permits shared-buffer double counting and the measured overstatement was 23.8x.
- Remove LZ4 IPC: rejected on current end-to-end evidence; the experiment remains a lab comparison, not an artifact change.

## Consequences

January TLC produces 16 rather than 46 deterministic segments. Durable publication fell from 363 ms to 142 ms and package execution from 1.617 s to 1.363 s in the live HTTPS comparison. Package hashes intentionally change under v2. Memory admission can now support zero-copy IPC without false oversized-window failures. Old serialized plans and package expectations are deliberately unsupported.

This decision supersedes `.10x/decisions/superseded/p3-initial-batch-segment-targets.md`, `.10x/decisions/superseded/adaptive-microbatch-canonical-segmentation.md`, and `.10x/decisions/superseded/canonical-segmentation-plan-artifact-gate.md`.
