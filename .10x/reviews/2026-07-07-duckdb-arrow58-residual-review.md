Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-p0-workstream-d-dependency-tuple-residual.md, .10x/tickets/done/2026-07-07-duckdb-arrow58-transitive-residual.md, .10x/evidence/2026-07-07-duckdb-arrow58-residual-audit.md, .10x/decisions/duckdb-arrow58-private-driver-residual.md, .10x/knowledge/datafusion-cratesio-arrow59-tripwire.md, .10x/specs/conformance-governance-roadmap.md, deny.toml, supply-chain/config.toml
Verdict: concerns

# DuckDB Arrow 58 residual review

## Findings

Significant: Workstream D is not closure-ready while the target tickets still carry an explicit blocker. Both `.10x/tickets/done/2026-07-07-p0-workstream-d-dependency-tuple-residual.md` and `.10x/tickets/done/2026-07-07-duckdb-arrow58-transitive-residual.md` state that terminal closure/move is deferred because active-path references outside the workstream's owned write scope still need repair. This review does not clear that blocker. Closure needs either broader reference repair or an explicit record-backed decision that these active-path references are acceptable without moving the ticket.

Significant: The governance publication constraint is record-backed but not mechanically enforced. `.10x/specs/conformance-governance-roadmap.md` says CDF MUST NOT publish crates.io releases while the DataFusion git pin remains, and the tripwire records the migration path. However, inspection found no `publish = false` guard in `Cargo.toml` or crate manifests, no `.github` release workflow to block, and no release automation/procedure that checks this stop-line. Cargo will naturally reject publishing crates that contain disallowed git/path dependency shapes, but lower-level crates that do not depend on the git-pinned engine can still be independently publishable. If Workstream D's closure claim is "publication constraint is recorded", the record satisfies it; if the claim is "accidental crates.io release is prevented", this is still a closure blocker or requires explicit residual-risk acceptance.

Minor: `.10x/specs/conformance-governance-roadmap.md` has the new DataFusion-git publication stop-line, but its header still says `Updated: 2026-07-05`. That is stale relative to the 2026-07-07 workstream update and should be repaired before clean closure if record metadata coherence is part of the closure bar.

No finding: I could not falsify the Arrow 58 private-driver claim. `cargo tree --workspace --locked -i arrow-array@58.3.0` shows the Arrow 58 path entering through `duckdb 1.10504.0 -> cdf-dest-duckdb`; `cargo tree --workspace --locked -i datafusion@54.0.0` shows DataFusion entering through `cdf-engine`; and `cargo metadata --locked --format-version 1` shows DataFusion from the pinned Apache git rev while Arrow/DataFusion engine packages resolve on Arrow 59.1.0. Source search found no `duckdb::arrow`, `query_arrow`, `stream_arrow`, `appender-arrow`, `vtab-arrow`, or `vscalar-arrow` use in owned DuckDB destination paths. The destination code reads CDF package `RecordBatch` values through Arrow 59 APIs, lowers them to `duckdb::types::Value`, and uses SQL rows, scalar query values, and JSON receipts across the DuckDB driver boundary.

No finding: The private-residual decision does not weaken the DataFusion day-zero requirement or the public one-tuple policy. It explicitly preserves DataFusion as the engine boundary, keeps the git pin time-boxed, forbids exposing `duckdb::arrow` / Arrow 58 structs from public CDF APIs, and lists revisit triggers for any DuckDB Arrow API use, package replay/receipt boundary change, new DuckDB release, supply-chain finding, or CDF minor tuple review.

No finding: The DataFusion crates.io tripwire is actionable and scoped to the intended source of truth. `cargo info datafusion --registry crates-io` currently reports `datafusion 54.0.0`, and the documented temporary-manifest check currently resolves `datafusion 54.0.0` with `arrow-array 58.3.0`, so it does not trigger today. The trigger action correctly opens a migration ticket only for a crates.io DataFusion release resolving to Arrow 59.x and sends non-59 majors back to tuple shaping.

No finding: The supply-chain posture is explicit enough for the current dependency state under the records reviewed. `deny.toml` denies unknown registries and unknown git sources while allowing only crates.io plus the Apache DataFusion git URL. `supply-chain/config.toml` contains `safe-to-deploy` exemptions for the DataFusion 54.0.0 packages, `duckdb 1.10504.0`, and Arrow 58/59 crates, and the evidence record reports `cargo deny check` and `cargo vet --locked` passing. I did not rerun those longer gates in this review.

## Residual risk

This review used read-only inspection, `cargo tree`, `cargo metadata`, `cargo info`, and targeted source searches. It did not run test suites, `cargo deny check`, or `cargo vet --locked`; it relies on `.10x/evidence/2026-07-07-duckdb-arrow58-residual-audit.md` for those gate results.

Registry freshness was checked on 2026-07-07: `duckdb` remains `1.10504.0` and `datafusion` remains `54.0.0` on crates.io. These facts are temporal and must be rechecked at the next dependency review or release gate.

The worktree was dirty before review, including target records and implementation files. This review did not revert or edit any existing implementation or reviewed record.
