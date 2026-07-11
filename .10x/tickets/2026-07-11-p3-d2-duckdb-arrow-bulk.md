Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/2026-07-11-p3-d1-bulk-path-contract.md, .10x/tickets/2026-07-07-duckdb-arrow-bulk-load-triage.md, .10x/tickets/2026-07-11-p3-a4-injected-execution-host.md

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
