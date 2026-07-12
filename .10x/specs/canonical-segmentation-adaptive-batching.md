Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Canonical segmentation and adaptive batching

## Purpose and scope

This specification governs execution microbatch adaptation, canonical output order/segment assembly, position joins/slicing, segment identifiers, identity evidence, dedup interaction, and jobs invariance.

## Plan contract

Every executable plan MUST record a segmentation policy version, canonical partition ordinals, segment-id namespace rule, row/byte targets, oversize behavior, and position algebra version. Exact defaults are calibrated by WS-L and remain deterministic for the plan.

Source capabilities MUST declare maximum poll working set and whether a batch can be split with exact row-range source positions. Estimates MAY influence a future explicitly versioned policy but MUST NOT silently alter canonical boundaries during execution. The active v2 policy is governed by `.10x/decisions/byte-first-canonical-segmentation-v2.md`.

## Execution microbatches

Microbatch adaptation MUST remain within plan min/max bounds and the memory ledger. It MAY respond to observed row width, pressure, spill, and downstream throughput. Internal boundaries and timing MUST remain outside package identity and MUST be rate-limited telemetry. Rebatching MUST preserve canonical rows, verdicts, and lineage.

The v2 execution range is 8,192–65,536 rows and 1–32 MiB. Canonical durable segments use an independent 32 MiB logical byte target, a 64 MiB hard byte maximum, and high row-count safety backstops. The assembler MUST NOT reuse a source/decode microbatch row ceiling as its segment ceiling.

## Canonical assembler

The assembler MUST process one partition's admitted stream in canonical order and MUST produce the same segments for jobs=1 and jobs=N. It MAY coalesce multiple inputs. It MAY split only with exact slice-position authority. It MUST flush conservatively when positions cannot be joined.

Every emitted segment MUST carry its partition ordinal/id, canonical segment ordinal, row count, byte count, content hash, and deterministic aggregate output position. Segment IDs MUST be derived from those ordinals by a versioned rule.

File-manifest positions join by deterministic file identity union within the same logical partition. Cursor/page/composite/foreign positions join only through their typed ordering/composition contract; unsupported joins force a boundary or plan failure. No generic JSON/string comparison is permitted.

Package-scoped dedup MUST follow `.10x/specs/spillable-package-dedup.md`, resolve canonical partition/row order before final segment assembly, and remain jobs/spill invariant.

## Identity and conformance

Canonical policy and emitted segments participate in package identity. Adaptive microbatch telemetry and wall pressure do not. Package replay consumes canonical segments without recomputing the policy.

Plans missing a canonical policy or carrying an unsupported version MUST fail closed. CDF has no compatibility requirement for pre-v2 plans or artifacts.

Permanent conformance MUST vary source batch/page sizes, jobs, channel pressure, destination speed, memory budget above the legal minimum, and spill timing while asserting identical package hashes/segments/positions. Narrow/wide/nested schemas, tiny files/pages, one oversized source chunk, quarantine, variant capture, limit, and exact-row dedup are required cases.

## Explicit exclusions

This spec does not permit cross-partition segments, scheduler-order IDs, inferred row cursors, identity-participating timing, or unbounded oversize batches.
