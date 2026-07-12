Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md, .10x/tickets/2026-07-07-duckdb-arrow-bulk-load-triage.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md

# P3 D2: Arrow-native DuckDB bulk writer

## Scope

Benchmark/implement the winning Arrow appender versus vtab/`INSERT SELECT` path, stream batches through a declared single-writer blocking lane, eliminate package/row vectors, and retain truthful schema-specific scalar compatibility only where necessary.

## Acceptance criteria

- Eligible TLC/nested/type-matrix schemas use Arrow-native batches and meet ≥1M rows/s and ≥5x baseline.
- Single-writer confinement does not block I/O/CPU upstream and participates in CPU/memory admission.
- Append/replace/merge, duplicate, correction, abort, receipt, and jobs laws match existing semantics.
- Scalar fallback is preplanned, field-explained, measured, and absent for the envelope schema.

## Evidence expectations

Appender/vtab comparison, dependency/feature review, type matrix, copy/allocation profile, transactions/crash conformance, and before/after envelope report.

## Explicit exclusions

No generic runtime or other destination branches.

## Blockers

Depends on D1, A4, and the DuckDB triage.

## References

- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/duckdb-arrow-c-stream-version-bridge.md`

## Progress and notes

- 2026-07-11: Source inspection rejected the binding's vtab helper because it permanently retains every passed batch. Ratified a narrowly isolated Arrow C Stream ABI bridge for the pinned Arrow 59/58 tuple.
- 2026-07-11: Implemented bounded native data-chunk append, deleted package/row vectors and scalar ingestion, moved provenance constants into vectorized per-segment SQL, and preserved transactional append/replace/merge, duplicate, correction, abort, receipt, and verification behavior.
- 2026-07-11: `.10x/evidence/2026-07-11-p3-d2-duckdb-arrow-milestone.md` records 1.946M TLC-shaped rows/s and 4.44x versus the exact removed scalar shape. The ≥1M row is green; D2 remains open for the final ≥5x gap, blocking-lane graph integration, and expanded type matrix.
- 2026-07-11: Expanded the exact Arrow appender path and truthful sheet to Decimal128 (precision <= 38, nonnegative scale), list/large-list/fixed-size-list, struct, map, and fixed-size binary. Decimal256 remains explicitly unsupported because the pinned binding maps it through lossy DOUBLE. A live appender test persists Decimal128, list, and struct batches; map planning is recursively type-checked.
