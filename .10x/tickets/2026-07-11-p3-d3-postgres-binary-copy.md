Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md

# P3 D3: Arrow-to-Postgres binary COPY writer

## Scope

Implement PostgreSQL binary COPY encoding directly from Arrow into bounded stage/attempt ingestion, delete scalar/text ingestion, and preserve transactional disposition/receipt semantics.

## Acceptance criteria

- Exact supported type matrix uses binary COPY with correct endian/epoch/decimal/array/null framing.
- Local throughput is ≥2x CSV baseline and remote profile is network/server-bound where expected.
- Stage/transaction lifecycle is bounded, invisible before final binding, rollback-safe, and duplicate-idempotent.
- Unsupported schemas fail preparation with field-level remediation; no text fallback is advertised or retained.

## Evidence expectations

Protocol/type golden vectors, PostgreSQL round trips, binary-vs-CSV benchmark, allocation/network profiles, transaction/crash/replay/correction conformance, and receipt verification.

## Explicit exclusions

No destination-generic PostgreSQL type assumptions.

## Blockers

Depends on D1 and staged-ingress contract.

## Progress and notes

- 2026-07-11: Removed the CSV compatibility path's package/segment scalar-row materialization. `PostgresPackageData` now retains bounded Arrow batches for the current verified segment; CSV fields are encoded directly into COPY one row at a time. All 30 unit/live transaction, merge, correction, rollback, receipt, and source tests remain green. This establishes the constant-memory encoder boundary that binary COPY will replace without changing commit semantics.
- 2026-07-11: Implemented PostgreSQL binary COPY framing directly over bounded Arrow batches, including exact null framing, integer/float endian encoding, byte/string transfer, PostgreSQL date/time/timestamp epochs, UInt64 and Decimal128/256 NUMERIC base-10000 encoding, and immutable row provenance. The release encoder control measured 36,443,900 binary rows/s versus 2,054,189 scalar CSV rows/s (17.74x). The live PostgreSQL suite passed append, replace, merge, rollback, correction, receipt, and decimal round trips. Under `.10x/decisions/pre-production-current-format-only.md`, removed production CSV COPY, scalar staging rows, their tests, and the unimplemented `extended_insert` capability; PostgreSQL now advertises only `copy_binary`.
- 2026-07-11: Added a bounded 1 MiB binary COPY aggregate buffer after measurement showed the synchronous PostgreSQL client flushes around 4 KiB, and compiled Arrow columns to typed encoder views once per batch rather than downcasting every cell. A server-inclusive 524,288-row TLC-shaped local benchmark now measures 1,662,005 binary rows/s versus 570,051 rows/s for the exact removed scalar CSV allocation/escaping shape (2.92x). The narrow three-field adversarial shape is server/wire-size bound near 2x because four provenance fields dominate; this limit is recorded in the milestone evidence rather than hidden.
- 2026-07-11: Replaced the final PostgreSQL segment staging container with `CommitSegment::into_batches()`. The prior public-field move dropped the verified segment's private memory-retention owner before binary encoding; the canonical iterator now holds the lease for the complete segment and each yielded batch. Deleted `PostgresPackageData`, `PostgresStageBatch`, `PostgresLoadedSegment`, schema rediscovery, and their vector assembly. All 30 active unit/live tests and strict Clippy remain green.

## References

- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/pre-production-current-format-only.md`
