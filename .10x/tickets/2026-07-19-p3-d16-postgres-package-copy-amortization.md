Status: active
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
- 2026-07-19: Static tracing found the amortization boundary in the kernel contract, not generic destination identity logic. `CommitSession::write_segment` forces a synchronous Postgres `CopyInWriter` borrow to open and finish once per segment; keeping that writer across calls would require a self-referential adapter or unsafe lifetime extension. The implementation will replace the finalized-package session method with one owned, bounded `CommitSegment` iterator returning per-segment acknowledgements. Generic finalized-package orchestration supplies the verified iterator once; destinations retain control of package-level protocol setup, while staged destinations remain on their separate ingress contract. Postgres can then open one COPY inside one blocking-lane call, consume and release segments sequentially, finish once, insert exact range mirrors, and commit the existing transaction. This is the smallest reusable boundary and deletes the superseded finalized per-segment call surface.
- 2026-07-19: Implemented the boundary ratified in `.10x/decisions/finalized-commit-session-bounded-segment-iterator.md`. `CommitSession` now accepts one owned verified segment iterator; generic replay constructs it once from the memory-accounted package reader and validates exact acknowledgement cardinality, canonical order, identity, and logical row counts before recording events. Postgres opens one `COPY ... FROM STDIN BINARY`, consumes and releases all segments through the existing bounded encoder, finishes COPY once, then writes exact segment-range mirrors and runs the existing atomic publication/receipt transaction. No destination identity branch, package materialization, callback, self-reference, unsafe code, or compatibility method remains.
- 2026-07-19: Local verification passed: workspace all-target check; strict touched-graph Clippy; `cdf-dest-postgres` library suite (`26 passed`, `2 ignored`) including append, replace, merge, duplicate replay, rollback, receipt, mirror, correction, and live transaction cells; five focused finalized replay/project tests; the exact generic acknowledgement negative law; and four destination/conformance registry tests. A broad concurrent `cdf-project` run reached `193 passed` and exposed five pre-existing global-fixture/schema-discovery failures unrelated to this diff; it is recorded only as a limit, not claimed as closure evidence.

## Blockers

None.

## Evidence

- API and boundedness: `CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets --locked -j 12` passed; source inspection finds one `copy_in` site in the Postgres package payload helper and no finalized `CommitSession::write_segment` method.
- Correctness: `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-postgres --lib --locked -j 12 --quiet` passed `26` tests with `2` explicit ignored performance probes. Focused `cdf-project` finalized replay, duplicate, failure/abort, staged-category, and acknowledgement-validation tests all passed. `cdf-conformance` destination laws and fourth-destination bulk enrollment passed.
- Quality: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-runtime -p cdf-project -p cdf-conformance -p cdf-cli -p cdf-dest-postgres --all-targets --locked -j 12 -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed.
- EC2 macro and direct-control evidence: pending.

## Review

Pending.

## Retrospective

Pending.
