Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md

# Make destination module boundaries explicit

## Scope

Replace production wildcard ownership edges in `cdf-dest-duckdb`, `cdf-dest-parquet`, and `cdf-dest-postgres` with explicit imports and explicit facade exports. Orient shared models below orchestration and normalize equivalent private role filenames (`plan`/`planning`, `correction`/`corrections`) where doing so improves cross-adapter navigation without public renames.

## Non-goals

- No SQL, transaction, receipt, correction, ingress, concurrency, or destination capability change.
- No new shared abstraction merely to make the three adapters look identical.
- No changes to adapter-specific physical behavior.

## Acceptance Criteria

- The three destination crates contain no production `use crate::*`, `use super::*`, or sibling wildcard imports.
- Their crate-root public facades enumerate all intended exports; public API is preserved.
- Import direction exposes an acyclic internal ownership graph rather than a crate-root hub.
- Equivalent private module roles use consistent names, or the executor records why a semantic difference justifies a different name.
- Focused destination, receipt, replay, correction, and conformance tests plus strict lint pass.

## References

- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/specs/destination-common-services.md`

## Assumptions

- Record-backed: adapter specificity belongs inside each destination crate while shared semantics remain in neutral contracts.
- Record-backed: private module filenames are not public API.

## Journal

- 2026-08-01: Audit found 30 top-level production wildcard imports across the three destination crates and wildcard public facades in each root.
- 2026-08-01: Execution started. Read the owning ticket and all referenced active records; confirmed the work is import/facade and private-layout cleanup only, with destination behavior and public symbol names held invariant.
- 2026-08-01: Replaced production wildcard imports throughout all three crates with direct external, sibling, or leaf-model imports. Replaced each crate-root wildcard facade with an explicit list of the symbols that the former facade exposed.
- 2026-08-01: Making dependencies explicit exposed real ownership cycles that the crate-root hubs had hidden. Added private `models.rs` leaves for destination configuration and value types, kept orchestration in `api`, `sheet`, `staging`, and correction modules, and moved the DuckDB commit writer to its owning segment-scan implementation. Moved Postgres type-fragment validation to `identifiers`, where the identifier policy is owned, eliminating the `identifiers`/`validate` cycle.
- 2026-08-01: Normalized the private Postgres filename `correction.rs` to `corrections.rs`, matching the role name in the other destination crates. Retained DuckDB `planning.rs` and Postgres `plan.rs`: DuckDB's module owns the writer-lock planning lifecycle, while Postgres's module owns public serializable plan value models, so the names describe intentionally different roles rather than equivalent seams.
- 2026-08-01: `graphify` was not present on `PATH`, so neither the requested query nor post-edit `graphify update .` could run. Used compiler-resolved imports, source inspection, and strict all-target lint as the dependency-graph evidence instead.
- 2026-08-01: The focused library-test command compiled the Rust test targets through linking, then the local linker failed because no native DuckDB library was available (`library not found for -lduckdb` and unresolved DuckDB C symbols). No Rust test failure was observed, but the tests did not execute; this remains a verification limit for integration with a DuckDB-enabled environment.
- 2026-08-01: Closure repair addressed the four ownership seams named by review without changing public symbols or destination behavior. DuckDB configuration-option rendering moved from `api` to the existing native-resource model leaf. Parquet staging/publication metadata moved to `models`, and the persisted manifest, provenance, and replace-pointer choreography moved intact from API orchestration to a focused private `publication` leaf consumed by both API and staging. Postgres correction capabilities moved to `corrections`, load-mirror row decoding moved to `mirrors`, and binary-COPY I/O classification moved into `binary_copy`.
- 2026-08-01: A direct reverse-edge probe initially showed the four reviewed pairs oriented one way, but the first executable Tarjan pass exposed two longer paths carrying the same ownership defects: Parquet `api -> runtime -> staging -> api` and Postgres `commit -> sheet -> corrections -> commit`. Moving publication operations and mirror-row decoding to their semantic leaf owners removed those indirect cycles. The same full-graph check now reports no multi-module SCC in any scoped crate.
- 2026-08-01: Post-repair all-target compilation and strict Clippy passed. A non-privileged focused unit-test attempt still failed while linking the Postgres test binary because workspace test linkage pulls in `cdf-dest-duckdb` and the environment has no `-lduckdb`; the chained command stopped before any selected Postgres or Parquet assertion ran. No privileged retry was attempted. The ticket remains active for fresh independent review.
- 2026-08-01: The dependent crate-documentation polish review found that DuckDB's lock-only `planning.rs` filename remained too broad. Its authorized follow-up renamed that private module to `writer_lock.rs`; this refines navigation without changing the earlier conclusion that DuckDB's guard lifecycle and Postgres's serializable `plan.rs` models are not equivalent roles.
- 2026-08-01: Parent integration ran the complete three-crate library suites with the documented `DUCKDB_DOWNLOAD_LIB=1` linkage outside sandbox restrictions. DuckDB passed 58/58; Parquet passed 46 with its existing release benchmark ignored; Postgres passed 35, including its live transactional tests, with two existing release benchmarks ignored.
- 2026-08-01: Reconciled the repair journal's unreproducible Parquet edge total against the independent re-review: the deduplicated production graph has 34 unique module edges, not 35. Both graph reconstructions agree there are no multi-module SCCs; only the count convention/evidence was corrected.

## Blockers

None.

## Evidence

- Production wildcard criterion: `rg -n '^\s*(pub\s+)?use\s+[^;]*::\*' crates/cdf-dest-duckdb/src crates/cdf-dest-parquet/src crates/cdf-dest-postgres/src --glob '*.rs'` reports only `use super::*` inside crate test aggregators or nested `#[cfg(test)]` modules. There are no production wildcard ownership edges or wildcard facade exports.
- Public facade criterion: the former wildcard-source public declarations were enumerated and preserved explicitly in each `lib.rs`. `cargo check -p cdf-builtin-drivers --lib --locked --message-format short` passed, proving the primary downstream facade consumer still compiles.
- Ownership-direction criterion: shared destination, correction, commit, and receipt value models now live in private leaf `models.rs` modules; orchestration imports those leaves directly. `cargo clippy -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --all-targets --locked -- -D warnings` passed, including all three libraries and their test targets, as well as affected `cdf-builtin-drivers` and `cdf-conformance` targets.
- Naming criterion: Postgres now uses `corrections.rs`; DuckDB's private lock guard is accurately named `writer_lock.rs`, while Postgres's distinct serializable value-model authority remains `plan.rs`.
- Hygiene: `cargo fmt -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres -- --check` and scoped `git diff --check` both passed.
- Test limit: `cargo test -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --lib --locked` reached native linking but could not execute without a local DuckDB library. Strict all-target Clippy proves the Rust test code compiles; it does not prove runtime assertions.
- Closure-repair graph proof: an inline Python checker enumerated every top-level production `src/*.rs` module (excluding `lib.rs`, `tests.rs`, and `live_tests.rs`), parsed direct and grouped `crate::` ownership edges, and ran Tarjan's strongly connected component algorithm. It exited zero with `cdf-dest-duckdb: 16 production modules, 41 internal edges, multi-module SCCs: []`; `cdf-dest-parquet: 13 production modules, 35 internal edges, multi-module SCCs: []`; and `cdf-dest-postgres: 15 production modules, 56 internal edges, multi-module SCCs: []`. The reviewed directions remaining are one-way: DuckDB `api -> segment_scan`, Postgres `sheet -> corrections`, and Postgres `commit -> binary_copy`; Parquet staging and API now both point to `publication` rather than each other.
- Reconciled graph evidence: the independent re-review's deduplicated parser reports DuckDB 16 modules/41 unique edges, Parquet 13/34, and Postgres 15/56, with no multi-module SCCs. The original transient checker was not retained and its Parquet count of 35 is superseded by the reproducible unique-edge count; the acyclicity conclusion is unchanged.
- Closure-repair compile/lint evidence: `cargo check -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --all-targets --locked` completed all scoped crates and affected downstream targets. After removing the two unused imports it identified, `cargo clippy -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --all-targets --locked -- -D warnings` passed on the final source. This proves the libraries and all test targets compile against the repaired ownership graph; it does not substitute for runtime assertions.
- Closure-repair hygiene: final scoped `cargo fmt -- --check` and scoped `git diff --check` passed. `graphify update .` was attempted conditionally after the repair but the executable remains unavailable, so no graph artifact refresh is claimed.
- Closure-repair runtime-test limit: `cargo test -p cdf-dest-postgres binary_copy_ --lib --locked && cargo test -p cdf-dest-postgres correction_plan_ --lib --locked && cargo test -p cdf-dest-parquet replace_duplicate_replay_requires_immutable_settlement_identity --lib --locked` failed at the first test binary's native link with `library not found for -lduckdb`; because the commands were chained with `&&`, none of the selected assertions executed. This reproduces the existing environment limit and does not challenge behavior.
- Closure runtime evidence: `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --lib --locked -j 12` completed successfully outside sandbox restrictions. Results: DuckDB 58 passed/0 failed; Parquet 46 passed/0 failed/1 pre-existing benchmark ignored; Postgres 35 passed/0 failed/2 pre-existing benchmarks ignored. This closes the focused destination/receipt/replay/correction/runtime-test criterion.

## Review

### Findings

- **Significant — the production ownership graph is still cyclic in all three destination crates, so the acyclic-import acceptance criterion is not met.** DuckDB's orchestration module imports the segment-scan implementation (`crates/cdf-dest-duckdb/src/api.rs:44`), while that implementation reaches back into orchestration for configuration (`crates/cdf-dest-duckdb/src/segment_scan.rs:291`). Parquet's API deserializes staging-owned metadata (`crates/cdf-dest-parquet/src/api.rs:199`, `:243`, and `:302`), while staging imports API-owned receipt/finalization functions (`crates/cdf-dest-parquet/src/staging.rs:23`). Postgres retains two direct back-edges: `sheet` imports correction orchestration (`crates/cdf-dest-postgres/src/sheet.rs:15`) while corrections import sheet capability construction (`crates/cdf-dest-postgres/src/corrections.rs:42`), and `commit` imports the binary-copy encoder (`crates/cdf-dest-postgres/src/commit.rs:22`) while the encoder imports commit-owned error conversion (`crates/cdf-dest-postgres/src/binary_copy.rs:15`). The crate-root wildcard hubs are gone and the extracted models point downward, but these strongly connected seams still make lower-level implementation modules depend on orchestration siblings.
- **Significant — the focused runtime-test acceptance criterion remains unproven.** The recorded `cargo test` command reached native linking and then failed before executing any assertion because the environment lacked DuckDB native symbols. All-target Clippy establishes that the test targets compile, and inspection found protective assertions for transaction ordering, rollback, duplicate replay, receipt identity, correction rollback/abort, and identifier rejection, but compilation is not evidence that those runtime behaviors pass after the moves.

### Verdict

**Fail.** No public-facade drift or SQL, transaction, receipt, correction, replay, or identifier-validation semantic change was found: the explicit root exports match the former public declarations, the Postgres `correction.rs` to `corrections.rs` move changes only imports/type placement, and `validate_type_fragment` retains its prior body and constructor call sites under `identifiers`. Production wildcard imports are absent. Closure is nevertheless blocked by the remaining production dependency cycles and by the unexecuted focused tests.

### Residual Risk

- Behavioral parity is supported by source-level function-body comparison and compiled protective test targets, not by executed destination tests in a DuckDB-enabled environment.
- The downstream `cdf-builtin-drivers` facade check covers the primary in-workspace consumer; it cannot prove compatibility for external consumers beyond the enumerated public-symbol comparison.

### Closure-repair re-review — 2026-08-01

This was a fresh, non-authoring review of the authorized closure repair. The reviewer used the repository's delegated open-code-review workflow to resolve the full three-crate production scope and destination-specific hostile-review rules, then independently reconstructed the production import graph rather than trusting the repair journal's SCC conclusion. No tests were rerun; the executor's compile/lint evidence and native-link limitation remain the ticket's authoritative observations.

#### Findings

- **Minor — the recorded Tarjan edge-count evidence is not exactly reproducible.** The repair journal retains the algorithm description and outputs, but not the inline checker itself, so its parsing rules cannot be inspected. An independent source parser plus Kosaraju SCC pass reproduced DuckDB's 16 modules/41 unique internal edges and Postgres's 15/56, but found 13 modules/34 unique internal edges for Parquet: 31 edges from top-level explicit imports plus the three additional qualified production references `runtime -> package`, `runtime -> staging`, and `store -> package`. The recorded Parquet total is 35. This does not challenge the architectural result: both independent graph passes found no multi-module SCC, and the intended directions are plainly one-way in source. It does mean the exact `35 internal edges` datum should not be treated as reproducible evidence without the original parser or an explanation of its edge-count convention.
- **Significant, carried forward — focused runtime assertions still have not executed.** The closure repair did not resolve the previously recorded native DuckDB link limitation. All-target check/Clippy evidence proves that the moved code and test targets compile, but the ticket's focused runtime-test acceptance criterion remains unproven in this environment.

#### Falsification evidence

- The independent production graph excluded `lib.rs`, `tests.rs`, and `live_tests.rs`, expanded grouped top-level `crate::` imports, included direct qualified `crate::<module>` references, deduplicated directed module pairs, and ran Kosaraju rather than repeating Tarjan. It found `[]` multi-module SCCs for all three crates. The repaired seams are downward: DuckDB `api -> segment_scan` while both consume `models`; Parquet `api -> publication`, `staging -> publication`, and `runtime -> staging`; Postgres `sheet -> corrections`, `commit -> mirrors`, and `commit -> binary_copy`, with no reverse path from the leaf owner.
- Production wildcard search found no `use crate::*`, `use super::*`, sibling `::*`, or root `pub use ...::*` in the three production graphs. Every remaining wildcard is confined to a `#[cfg(test)]` nested module or the excluded test aggregators.
- The current explicit root facades were compared to every public declaration formerly exported through each `api::*`, `correction::*`, `identifiers::*`, `plan::*`, or `sheet::*` glob. DuckDB and Parquet re-export the same prior public API/model symbols from `models`; Postgres's explicit lists cover every formerly glob-exported symbol. No private publication, mirror, COPY, or writer-lock helper leaked into a public facade.
- DuckDB's moved `duckdb_config_options` retains the same six names, values, order, and `CdfError` behavior, and both bounded connection paths consume that leaf helper (`models.rs:42-57`, `segment_scan.rs:291-301`). `writer_lock.rs` is the prior lock implementation with explicit imports, and `DuckDbCommitWriter` now lives with the segment-scan lifecycle it owns. The only in-body unsafe scan change replaces an FFI wildcard with an explicit symbol list; type mapping and calls are unchanged.
- Parquet's `publication.rs` preserves the prior operation order and failure rails: current mutation assertion, injected receipt time, immutable provenance create/readback, canonical manifest create-or-verify, current-pointer CAS with the same 32-attempt bound and exact readback, immutable settlement verification, then receipt construction (`publication.rs:17-215`). Call sites changed only from associated-method or API imports to the publication leaf; SQL is not involved.
- Postgres's `corrections.rs` differs from the former `correction.rs` only in explicit imports, moved model declarations, and the byte-identical capability constructor now owned beside correction orchestration. The transaction statement order and correction body are unchanged. `decode_postgres_load_row` moved from `commit` to `mirrors`; its replacement `load_count` and inline JSON mapping preserve the old `CdfError::data` classifications and evidence comparisons (`mirrors.rs:18-63`). COPY's `io_error` moved into `binary_copy` with the same `CdfError::destination` body (`binary_copy.rs:20-22`). Diff inspection found no SQL literal, transaction boundary, receipt field, correction step, or COPY encoding change.

#### Verdict

**Concerns.** The authorized ownership repair itself passes: the production module graphs are acyclic, leaf ownership and filenames are accurate, root facades do not drift, production globs are gone, and no destination semantic change was found. Ticket closure should nevertheless remain blocked on the already-known focused-runtime-test evidence gap. The non-reproducible Parquet edge total is a minor evidence-quality concern and does not require a source repair.

#### Residual Risk

- Runtime parity remains supported by line-for-line/source-equivalent move inspection and compiled all-target test code, not executed destination assertions in a DuckDB-enabled environment.
- Source-level import extraction can under- or over-count unconventional macro-generated dependencies. This review mitigated that limit with two independent parsers/algorithms and direct inspection of every repaired seam; Rust compilation remains the authority that the resolved paths exist, while neither method proves runtime behavior.
- External public-API compatibility remains bounded by explicit symbol enumeration and the executor's downstream compilation evidence; semver tooling was not run.

### Final closure review — 2026-08-01

#### Findings

None. The two concerns carried by the closure-repair re-review are closed by the subsequently recorded integration evidence.

#### Reconciliation

- The graph-evidence concern is closed. The ticket now supersedes the transient Parquet count of 35 with the independently reproducible deduplicated count of 34 unique production module edges. Both independent reconstructions agree on 13 Parquet production modules and no multi-module SCC; DuckDB remains 16/41 and Postgres 15/56, also with no multi-module SCC. The repaired ownership directions and direct seam inspection support the same acyclic conclusion.
- The runtime-evidence concern is closed. The complete linked command executed every non-ignored library test in the three scoped crates: DuckDB 58 passed/0 failed, Parquet 46 passed/0 failed with one pre-existing benchmark ignored, and Postgres 35 passed/0 failed with two pre-existing benchmarks ignored. This includes the destination, receipt, replay, correction, COPY, publication, transaction, and live Postgres protections relevant to the ownership-only moves.
- The remaining acceptance criteria were already supported by the final source and compiler evidence: no production wildcard ownership imports or facade globs, explicit public exports matching the former wildcard surfaces, strict all-target lint, accurate private role names, and clean formatting/hygiene.

#### Verdict

**Pass.** The closure repairs address the original cyclic-ownership findings, the corrected graph evidence is internally consistent and independently reproduced, the complete destination suites pass, and no public-facade or destination-behavior drift remains. All ticket acceptance criteria are supported. Status remains active for parent closure judgment.

#### Residual Risk

- The three pre-existing release benchmark tests remained ignored, so this closure proves ordinary destination correctness rather than performance behavior.
- External public-API compatibility is supported by exact symbol enumeration and downstream workspace compilation, not a separate semver-diff tool.
- The graph result is source-derived; macro-generated dependencies could evade a source parser. Direct seam inspection, two independent SCC passes, and successful all-target compilation materially bound that risk.

## Retrospective

- Wildcard imports were not merely stylistic debt: they concealed cyclic ownership and made orchestration modules appear to own shared value types. Replacing them first let the compiler expose the actual graph, after which extracting leaf-owned models was smaller and safer than guessing at a target layout up front.
- Compiler-guided import repair plus strict all-target lint was effective at preserving private test access while shrinking production dependency edges. Native-library availability must still be part of the integration environment before runtime destination assertions can serve as closure evidence.
- A direct two-file edge probe is insufficient closure evidence: removing a reviewed back-edge can leave an indirect cycle through a third module. Running the full SCC algorithm before compile/lint exposed the remaining Parquet and Postgres paths and led to better owners (`publication` for persisted object publication, `mirrors` for mirror-row decoding) instead of merely hiding calls behind associated methods.
