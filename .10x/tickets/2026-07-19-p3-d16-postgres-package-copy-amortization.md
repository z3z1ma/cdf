Status: open
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d3-postgres-binary-copy.md, .10x/tickets/done/2026-07-18-p3-d15-canonical-package-row-ordinal.md

# P3 D16: amortize Postgres package COPY ingestion

## Scope

Remove the full-product gap between Postgres's fast Arrow-to-binary encoder and its segment-scoped destination lifecycle. A package transaction MUST amortize COPY setup and wire flushing across canonical segments while preserving bounded input, per-segment acknowledgement evidence, package-atomic append/replace/merge, rollback, receipts, and transaction-owned row-key allocation.

## Non-goals

- No text/CSV fallback.
- No generic runtime branch naming Postgres.
- No materialized full-package buffer.
- No weakened segment identity or early target visibility.

## Acceptance Criteria

- One destination-owned package ingest session amortizes protocol setup across segments, or a measured alternative reaches the same outcome without one COPY handshake per segment.
- `_cdf_row_key` remains `allocated_start + _cdf_package_row_ord`; no row-index regeneration returns.
- The full-year 41,169,720-row EC2 cell improves by at least 2x over `102.702915347s` and targets at least 1M rows/s end to end; no slower default is retained.
- The direct server-inclusive binary-vs-CSV control remains at least 2x, with encoder/send/final-publication timing separated.
- Append, replace, merge, duplicate replay, abort/rollback, receipts, mirrors, corrections, and bounded-memory conformance remain green.

## References

- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/canonical-package-row-ord.md`
- `.10x/evidence/.storage/2026-07-19-p3-d15-postgres-full-year-current.json`
- `.10x/tickets/done/2026-07-11-p3-d3-postgres-binary-copy.md`

## Assumptions

- Record-backed: the current full-product destination phase is `98.526859220s` for 215 canonical segments, while the same current encoder/server control reaches `1,375,614` binary rows/s and `3.33x` its CSV control.
- Record-backed: package atomicity requires one transaction but does not require reopening PostgreSQL COPY for every canonical segment.
- User-ratified: performance regressions are not retained as defaults; tuning values remain knobs rather than hidden hard caps.

## Journal

- 2026-07-19: Opened from D15's controlled cross-destination closeout. The full product completed correctly and without memory pressure, but only at `400,862` rows/s; the direct current binary COPY control proves the encoder/server path is more than three times faster. This ticket owns the lifecycle/amortization gap and must not reintroduce deleted scalar or destination-local provenance code.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
