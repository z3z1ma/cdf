Status: cancelled
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/done/2026-07-10-p3-ws-d-destination-bulk-paths.md
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
- 2026-07-19: The first clean EC2 cell falsified COPY-handshake amortization as sufficient. One package-wide COPY still completed in `103.398522104s` (`398,165` rows/s), statistically unchanged from the `102.702915347s` baseline; `destination_write_receipt` remained `99.800974214s`. Live PostgreSQL activity showed the COPY had completed and the server spent the remaining time in `INSERT INTO target SELECT ... FROM stage`. The destination was writing append payload twice; this evidence is retained at `.10x/evidence/.storage/2026-07-19-p3-d16-single-copy-stage-smoke.json` rather than being rationalized as a win.
- 2026-07-19: Removed the redundant stage for append and replace. Their plan now contains one typed binary-COPY operation directly into the target inside the same package transaction; replace counts and truncates the target in that transaction before COPY. Merge alone retains an optional stage because deduplication, conflict counting, and `ON CONFLICT` publication require it. The provenance uniqueness index is built after payload publication in the same transaction, allowing a new target's initial bulk load to avoid row-by-row B-tree maintenance while preserving the exact committed constraint. Append/replace plans no longer carry a dormant stage-table value or superseded stage-to-target SQL. The full live Postgres suite and strict Clippy pass after this current-only cleanup.
- 2026-07-19: Clean EC2 evidence retains the direct-target design but falsifies the ticket's 2x full-product stretch target. On the same `c7i.4xlarge`, tuned gp3 host, the comparable warm cell improved from the single-COPY/staged-publication `103.398522104s` / `398,165` rows/s to direct-target `76.808883659s` / `536,002` rows/s, a `1.346x` speedup and `25.7%` wall-time reduction. That warm cell was diagnostic only: `run-cell` performs an unmeasured warm-up, and the external Postgres target therefore contained exactly `82,339,440` rows after the measured append. A corrected single-sample, uncontrolled-I/O run against a newly recreated database completed in `61.216020108s` / `672,531` rows/s with exactly `41,169,720` target rows, one provenance index, no spill event, and `3.995 GiB` peak process RSS. The fresh-target result has no same-protocol pre-change control and is therefore reported as a standalone product observation, not compared numerically with the contaminated `102.702915347s` warm baseline.
- 2026-07-19: Bounded floor isolation rejected another lifecycle rewrite. Dropping and rebuilding the exact unique provenance index over the 41,169,720-row target took `8.62s`; subtracting that from the `57.694s` destination phase leaves roughly `49.1s` in durable logged binary COPY, row-protocol encoding, and server insertion. There is no remaining append/replace stage copy, per-segment COPY handshake, target-index maintenance on the fresh load, or generic-runtime destination branch. Further material improvement requires a distinct measured design such as parallel ordered COPY encoding or an explicit lower-durability target policy, not another D16 lifecycle patch. The direct encoder/control code is unchanged from D15's `1,375,614` rows/s and `3.33x` CSV evidence. Per the performance-first no-thrash rule, the comparable `25.7%` indexed-append wall reduction is retained while D16 closes cancelled because its 2x criterion was not met.

## Blockers

None.

## Evidence

- API and boundedness: `CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets --locked -j 12` passed; source inspection finds one `copy_in` site in the Postgres package payload helper and no finalized `CommitSession::write_segment` method.
- Correctness: `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-postgres --lib --locked -j 12 --quiet` passed `26` tests with `2` explicit ignored performance probes. Focused `cdf-project` finalized replay, duplicate, failure/abort, staged-category, and acknowledgement-validation tests all passed. `cdf-conformance` destination laws and fourth-destination bulk enrollment passed.
- Quality: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-runtime -p cdf-project -p cdf-conformance -p cdf-cli -p cdf-dest-postgres --all-targets --locked -j 12 -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed.
- EC2 macro and direct-control evidence: the direct encoder/server control remains the unchanged D15 `1,375,614` rows/s / `3.33x` CSV result. The product evidence below separates the staged-publication null, indexed steady append, corrected fresh target, and isolated provenance-index cost.
- `.10x/evidence/.storage/2026-07-19-p3-d16-single-copy-stage-smoke.json`: clean `e94b5fbd` single-COPY/staged-publication falsification, configured 41,169,720-row workload, `103.398522104s`, `398,165` rows/s, no spill. This warm cell is comparable to the direct-target warm diagnostic but is not fresh-target evidence because its unmeasured warm-up shared the external database. Live server activity proves the stage-to-target publication, not COPY handshakes, dominated that path.
- `.10x/evidence/.storage/2026-07-19-p3-d16-postgres-direct-copy-smoke.json`: clean `2060c0a6` warm-cell diagnostic, `76.808883659s`, `536,002` rows/s, `73.244697865s` destination phase. Limit: the warm-up mutated the external destination, so the measured sample appended into an already populated/indexed target; exact postcondition was `82,339,440` rows.
- `.10x/evidence/.storage/2026-07-19-p3-d16-postgres-direct-copy-fresh.json`: clean `2060c0a6` corrected single-run evidence against a recreated database, exact `41,169,720` rows, `61.216020108s`, `672,531` rows/s, `57.693843304s` destination phase, one provenance unique index, no spill event, and no memory-pressure event. Standalone index reconstruction measured `8.62s`; that shell timing is journal evidence, not part of the machine observation.

## Review

Verdict: **concerns for the original ticket target; pass for the retained implementation**.

- Critical/significant correctness findings: none. Fresh adversarial inspection confirms one destination-owned COPY per finalized package, bounded verified-segment iteration, exact acknowledgement validation, transaction-owned row-key allocation, direct append/replace publication, merge-only staging, post-payload provenance uniqueness in the same transaction, and no concrete-destination branch in generic orchestration. The live suite covers append, replace, merge, duplicate replay, rollback, receipts, mirrors, and corrections.
- Significant acceptance concern: the retained product path reaches `1.346x` on the comparable indexed-append cells, not the ticket's required 2x improvement. The `61.216s` fresh-target cell is not compared to the contaminated warm baseline. This criterion is not waived or relabeled as passing; the ticket closes cancelled.
- Minor benchmark-protocol finding: warm macro cells are not isolated from external mutable destinations. The contaminated result is retained only as an indexed steady-append diagnostic, while the uncontrolled single-run cell is the fresh-target authority. L7 records the protocol limit for subsequent destination measurements.
- Residual risk: the EC2 macro uses local PostgreSQL and no enforced cgroup `MemoryMax`; remote network-bound PostgreSQL remains outside this ticket. The direct target path is still strictly faster in both fresh and indexed-append observations, and no slower default was retained.

## Retrospective

The first hypothesis was wrong for a useful reason: amortizing 215 COPY handshakes did not move the macro because append data was still written twice. Live server activity exposed the second write immediately; deleting append/replace staging delivered the real improvement and left merge's semantic stage intact.

The benchmark itself then demonstrated a second failure mode: a generic warm-up is safe for read-only references and workspace-local disposable destinations, but not for an external database whose state survives child workspaces. Destination performance cells must assert external postconditions and either provide per-sample reset authority or use a single explicitly uncontrolled sample. The 82,339,440-row postcondition prevented a contaminated number from being promoted.

Finally, a stretch target is not evidence. Once the exact index cost showed the remaining floor was durable Postgres COPY/encoding rather than another lifecycle mistake, continuing to reshape product semantics would have violated the user's performance/no-thrash guardrail. Retain the measured 25.7% comparable indexed-append reduction, state the unmet 2x criterion, and move on.
