Status: active
Created: 2026-07-11
Updated: 2026-07-11

# P3 initial microbatch and canonical segment targets

## Context

The pre-optimization baseline used 1,024-row source batches and observed segment encoding at 31.535 ms, the largest measured instrumented phase for CDF NDJSON-to-package. Fixed per-segment work is therefore severely under-amortized. The ratified envelope bounds execution batches to 8k–64k rows and 1–32 MiB. Live pressure may tune microbatches but cannot change canonical package segmentation.

## Decision

Policy `canonical-segmentation-v1` uses a canonical target of 65,536 rows or 8 MiB, with hard maxima of 65,536 rows and 32 MiB. Adaptive execution microbatches use 8,192–65,536 rows and 1–32 MiB. Observed row width and managed-memory availability choose an execution target inside those bounds; the canonical target remains plan data and is unaffected by pressure.

The 64k row target maximizes amortization within the ratified range. The 8 MiB canonical byte target is large enough to amortize IPC/fsync/hash/event costs while allowing hundreds of in-flight batches under the ordinary managed pool. The 32 MiB ceiling prevents wide/nested schemas from turning row count into an unbounded allocation.

## Alternatives considered

- Preserve 1,024 rows: rejected by the measured fixed-cost dominance.
- Use 64k rows without a byte ceiling: rejected because wide rows violate constant memory.
- Make the canonical target react to pressure or destination speed: rejected because scheduling would change package hashes.
- Start at 32 MiB canonical targets: rejected until large-fixture roofline evidence proves the extra latency/memory improves throughput.

## Consequences

The exact policy is serializable and versioned. A3 may tune execution microbatches without artifact drift. The performance lab can supersede these targets only with measured steady-state evidence and a new policy version if canonical boundaries change.
