Status: done
Created: 2026-07-11
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md, .10x/tickets/done/2026-07-07-duckdb-arrow-bulk-load-triage.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md

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
- 2026-07-11: Replaced per-segment full-column provenance transfers for append/replace with one package-wide Arrow ingress and a compact three-scalar segment-range table, followed by one vectorized final provenance join. Equal-work release evidence is now 1.938M rows/s versus 332k for the removed scalar path (5.84x) with an 11.1M rows/s raw appender control. Both throughput criteria are green; the declared single-writer blocking-lane execution join remains before closure.
- 2026-07-11: Live TLC evidence showed the three-string physical provenance shape still dominated destination time despite the prior synthetic milestone. Ratified the shared compact-provenance decision and replaced DuckDB payload provenance with one transactionally allocated `UBIGINT` row key plus exact segment range mappings. The release micro-path is now 9.42M rows/s versus 11.36M raw and 470k scalar; the public TLC commit/receipt phase fell from 2.730s to 1.233s and total run from 5.14s to 3.47s. All active DuckDB tests pass. D2 remains open only for declared blocking-lane graph execution and equivalent native-ingest envelope integration. Evidence: `.10x/evidence/2026-07-11-p3-duckdb-compact-row-range-provenance.md`.
- 2026-07-11: Connected DuckDB to the neutral staged-durable-segment contract and its declared pinned blocking lane. Durable in-memory Arrow batches now enter one isolated transaction while package persistence continues; final verified binding validates exact ordered segment identities, writes the canonical receipt/provenance mirrors, and commits. The 2,964,624-row local TLC run fell from 2.52s to 1.96s end to end; post-package final binding fell from 1.258s to 0.252s. Package and destination work now overlap, so phase evidence must distinguish the overlapped pipeline interval from final binding. Evidence: `.10x/evidence/2026-07-11-p3-d2-duckdb-overlapped-staged-ingress.md`.
- 2026-07-11: Removed an artificial queue-depth serialization after a six-run control: one-segment/64 MiB staging measured median 2.15s; two-segment/128 MiB measured 1.89s (12.1% faster). The destination remains single-writer and globally memory-accounted; the extra window allows package persistence to stay ahead of that writer.
- 2026-07-11: Closed D2. A fresh release control measured 9,759,287 TLC-shaped rows/s versus 11,534,448 raw Arrow appender rows/s and 416,802 removed scalar-shape rows/s: 84.6% of the native control and 23.41x scalar. The full DuckDB suite passed 24 tests with the benchmark intentionally ignored in fast mode; strict all-target Clippy passed. Existing staged-ingress evidence proves the pinned single-writer lane overlaps upstream persistence under shared byte admission, and the type/disposition/correction/receipt suite covers the declared semantics. Evidence: `.10x/evidence/2026-07-11-p3-d2-duckdb-closeout.md`; review: `.10x/reviews/2026-07-11-p3-d2-closeout-review.md`.
