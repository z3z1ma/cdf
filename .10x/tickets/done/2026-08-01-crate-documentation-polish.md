Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md
Depends-On: .10x/tickets/done/2026-08-01-format-transport-crate-root-splits.md, .10x/tickets/done/2026-08-01-core-public-facade-exports.md

# Complete crate-root documentation polish

## Scope

Add concise crate-level documentation to the audited roots that lack it: `cdf-bench-core`, `cdf-benchmarks`, `cdf-format-arrow-ipc`, and the eight `cdf-transform-*` crates. Reconcile private destination module role naming after the destination-boundary child if any inconsistency remains.

## Non-goals

- No user-facing behavior, public symbol, dependency, or implementation change.
- No repetitive generated essays or documentation that restates every type.
- No removal of the intentionally parked `cdf-wasm` boundary.

## Acceptance Criteria

- Every library crate root begins with accurate crate-level documentation explaining its authority boundary.
- Equivalent destination private module roles use consistent names or have a recorded semantic rationale.
- Documentation tests/formatting and strict lint pass.

## References

- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/active-backlog-and-future-roadmap.md`

## Assumptions

- Record-backed: all manifests already have accurate short descriptions that can seed concise crate docs.

## Journal

- 2026-08-01: Ticket opened for the audit's low-severity documentation and naming polish.
- 2026-08-01: Execution started after the implementation dependencies completed. Read the ticket, both governing knowledge records, and the destination-boundary dependency ticket completely. Scope remains documentation-only plus a private destination role-name/rationale reconciliation; public symbols and behavior are frozen.
- 2026-08-01: Added concise authority-boundary crate docs to `cdf-bench-core`, `cdf-benchmarks`, `cdf-format-arrow-ipc`, and all eight `cdf-transform-*` roots. The docs distinguish shared measurement contracts from benchmark-suite orchestration, IPC decoding from transport/scheduling, and byte/character transformation from record parsing.
- 2026-08-01: Reconciled destination private-role naming against the completed sources. All three adapters now consistently use plural `corrections.rs` and leaf `models.rs`. DuckDB intentionally retains `planning.rs` because it owns the writer-lock acquisition lifecycle; Postgres intentionally retains `plan.rs` because it owns public serializable load-plan value models. Parquet has neither equivalent role. Renaming these semantically different modules would reduce navigational accuracy, so no destination source changed.
- 2026-08-01: `graphify query` was attempted before source inspection but the executable is absent from this environment; direct manifests, crate roots, dependency records, and destination role implementations supplied the documentation and naming authority instead.
- 2026-08-01: `cdf-benchmarks/src/lib.rs` already contained concurrent explicit-facade work when execution began. The documentation edit was limited to prepending its crate doc and preserved that existing diff unchanged.
- 2026-08-01: Scoped formatting, warnings-denied Rustdoc, documentation tests, and all-target strict Clippy passed for all eleven documented crates. `graphify update .` was attempted afterward and also failed with `command not found`; no graph output was changed.
- 2026-08-01: Closure repair accepted the review's narrower ownership evidence. DuckDB's private `planning.rs` contained only the `WriterLock` guard, atomic lock-file acquisition, and `Drop` cleanup, while commit planning remains in `api`/`sheet`. Renamed the module to `writer_lock.rs` and updated only its private declaration/import. Postgres `plan.rs` remains unchanged because it still owns serializable load-plan models; the modules are semantically distinct rather than naming peers.
- 2026-08-01: Post-rename all-target `cargo check` and strict Clippy passed for `cdf-dest-duckdb` and affected downstream targets. Scoped formatting and diff hygiene passed; no `planning` module reference remains. `graphify update .` was attempted conditionally but the executable remains unavailable. The ticket remains active for fresh independent review.

## Blockers

None.

## Evidence

- Root coverage: a first-line scan of `cdf-bench-core`, `cdf-benchmarks`, `cdf-format-arrow-ipc`, and all eight `cdf-transform-*` roots reports a crate-level `#![doc = ...]` attribute in every listed library. Manifest descriptions and the corresponding public drivers/contracts were inspected before wording each boundary.
- Documentation accuracy: benchmark-core docs assign shared measurement contracts, host probes, and child runners to that crate; benchmark-suite docs assign fixtures, comparisons, and report orchestration to `cdf-benchmarks`; Arrow IPC docs assign detection/discovery/planning/decode while excluding transport and scheduling; transform docs assign streaming decoding/normalization and bounded memory/expansion while excluding transport and record parsing.
- Destination naming: `cdf-dest-duckdb`, `cdf-dest-parquet`, and `cdf-dest-postgres` each use `corrections.rs` and `models.rs`. DuckDB's lock-only lifecycle now resides in private `writer_lock.rs`; Postgres `plan.rs` defines the public serializable load-plan, statement, drift-hook, and receipt-input value model. The filenames now state their distinct semantic owners without any public symbol change.
- Rustdoc: `RUSTDOCFLAGS='-D warnings' CARGO_BUILD_JOBS=12 cargo doc -p <all eleven scoped crates> --no-deps --locked -j 12` passed and generated the crate documentation without warnings.
- Documentation tests: `CARGO_BUILD_JOBS=12 cargo test -p <all eleven scoped crates> --doc --locked -j 12` passed. Each crate currently contains zero executable doctest examples, so this proves the documentation harness builds cleanly but not any runtime behavior.
- Strict lint: `CARGO_BUILD_JOBS=12 cargo clippy -p <all eleven scoped crates> --all-targets --locked -j 12 -- -D warnings` passed.
- Formatting and hygiene: scoped `cargo fmt --check` and `git diff --check` passed. The implementation edits add only crate documentation; the pre-existing concurrent explicit-export diff in `cdf-benchmarks/src/lib.rs` was preserved.
- Tooling limit: `graphify query` and `graphify update .` both failed because `graphify` is not installed on `PATH`; no graph freshness is claimed.
- Naming-repair verification: `cargo check -p cdf-dest-duckdb --all-targets --locked` and `cargo clippy -p cdf-dest-duckdb --all-targets --locked -- -D warnings` passed, including affected `cdf-builtin-drivers` and `cdf-conformance` targets. `cargo fmt -p cdf-dest-duckdb -- --check` and scoped `git diff --check` passed. A source scan finds `mod writer_lock` and `writer_lock::WriterLock`, with no Rust module reference to `planning`; remaining prose uses of “planning” describe the activity rather than a module.

## Review

Independent red-team review performed 2026-08-01 using the delegated OCR benchmark-contract and source/format-extension rules, the complete scoped diff, manifests, crate authority implementations, and the destination module map.

### Findings

- **Minor — DuckDB's retained `planning.rs` filename still does not describe its role.** The rationale correctly establishes that DuckDB `planning.rs` and Postgres `plan.rs` are not equivalent modules: Postgres owns the public serializable load-plan, statement, drift-hook, and receipt-input models, while DuckDB's file should not be renamed merely to match it. However, `crates/cdf-dest-duckdb/src/planning.rs` contains only the private `WriterLock` type, its filesystem-lock acquisition, and its `Drop` cleanup. Its call sites acquire that guard around empty/data commits and correction commit (`api.rs` and `corrections.rs`), not around plan construction. A role name such as `writer_lock.rs` or `locking.rs` would be more navigationally accurate. This is private layout debt rather than a behavioral or public-API defect, but it leaves the ticket's naming-polish objective incomplete.
- No crate-documentation accuracy defect was found. `cdf-bench-core` actually owns the shared report/observation/measurement models, host providers, and child-command runners described at its root. `cdf-benchmarks` owns the fixture catalog/generators, comparison and reference machinery, benchmark matrix/runners, and envelope/report production while invoking runtime and adapter authorities for production execution. The Arrow IPC crate implements file and stream detection, discovery, decode-unit planning, and decoding over runtime-owned byte-source/session contracts. Every transform document is supported by its driver descriptor and implementation: Brotli/bzip2 working-set and expansion enforcement, character encoding/BOM handling, gzip member/checksum validation, LZ4 frame/checksum validation, Snappy framed-chunk/checksum validation, concatenated xz decoding with memory/expansion bounds, and zstd frame/window/expansion bounds.
- Coverage is complete, including the intentionally parked WASM root. A workspace scan of every `crates/*/src/lib.rs` found crate-level documentation on the first line; the eleven roots named in this ticket all begin with the new `#![doc = ...]` boundary statement.
- No ticket-owned code, public-symbol, behavior, or dependency change was found. Ten scoped roots differ from `HEAD` only by the two-line crate-documentation addition. `cdf-benchmarks/src/lib.rs` also contains the explicit-facade replacement independently owned and already reviewed under `.10x/tickets/done/2026-08-01-core-public-facade-exports.md`; the documentation change itself is only its first two lines. No scoped `Cargo.toml`, workspace manifest, or `Cargo.lock` diff exists.
- Destination role reconciliation is otherwise accurate: DuckDB, Parquet, and Postgres all use plural `corrections.rs` and leaf `models.rs`; Postgres `plan.rs` is a public serializable value-model authority; Parquet has no corresponding plan/planning module. The minor DuckDB role-name concern above does not make these modules equivalent.
- The recorded validation is proportionate to a documentation-only edit. Warnings-denied Rustdoc, the documentation harness, scoped formatting, and all-target strict Clippy are recorded as passing for all eleven packages. Source inspection found no executable doctest blocks, consistent with the recorded zero-doctest result; that command proves documentation compilation rather than runtime behavior, as the ticket correctly states.

### Verdict

**Concerns.** The eleven crate docs are concise, complete, and accurate, and the ticket introduced no code, public API, or dependency change. The retained DuckDB `planning.rs` name is the sole concrete defect: the recorded non-equivalence rationale is sound, but the chosen filename still misstates a writer-lock-only responsibility.

### Residual Risk

- The documentation edit and the separately owned `cdf-benchmarks` facade edit share one uncommitted file, so ownership attribution relies on the two tickets and exact hunk inspection rather than commit isolation.
- The validation commands are summarized with an “all eleven scoped crates” placeholder and their raw output is not retained. The package set is reconstructible from Scope and the executor's journal is authoritative for its observation, but a cold reader cannot replay an exact copied command from this record alone.
- The destination-boundary ticket remains active with an independent fail verdict for ownership cycles. Those cycles do not invalidate this ticket's documentation or plan/planning non-equivalence conclusion, but later boundary repair could change the current private module map.

### Naming-repair re-review — 2026-08-01

This was a fresh, non-authoring review of the authorized DuckDB naming repair. The reviewer used the delegated open-code-review workflow to resolve the scoped documentation, destination, and durable-record rules, then inspected the current source, the former `planning.rs` body from `HEAD`, every live Rust reference, the owning and dependent records, all eleven documented crate roots, their manifests, and the public driver/benchmark authorities. No tests or build commands were rerun; the executor's recorded validation remains authoritative for what it observed.

#### Findings

- **Minor — the active parent program still describes the completed rename as awaiting authorization.** `.10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md:70-77` says “Repair authorization required” and still lists renaming DuckDB's lock-only `planning.rs`, although that same parent journal records the user's authorization and the source, this child, and the destination-boundary child all record the completed `writer_lock.rs` repair. Historical `planning.rs` text in this ticket's append-only journal and first review is properly superseded by the later repair entries, and the destination-boundary ticket likewise records both the earlier decision and later correction. The parent blocker is the only stale live instruction found; it should be reconciled during aggregate closure so a cold reader is not sent to repeat completed work.
- No source, public-API, behavior, or documentation defect was found in the repair. The concern above is orchestration-record bookkeeping outside the repaired Rust boundary.

#### Falsification evidence

- `writer_lock.rs` owns exactly the filesystem writer guard its name promises: `WriterLock`, atomic `create_new` acquisition, PID write/sync, and best-effort `Drop` cleanup. Commit construction remains in `api`/`sheet`, and Postgres's distinct `plan.rs` still owns the public serializable load-plan, statement, drift-hook, and receipt-input models. The rename therefore improves ownership navigation without manufacturing equivalence between the two adapters.
- A direct unified comparison of the former `planning.rs` from `HEAD` and current `writer_lock.rs`, beginning at `#[derive(Debug)]`, produced no diff. The type, acquisition branches and error classes/messages, write/sync order, returned guard, and `Drop` cleanup are byte-identical. The explicit import block belongs to the separately reviewed destination-boundary cleanup; the naming repair changes only the private module path.
- The old root declared private `mod planning`; the current root declares private `mod writer_lock`. `WriterLock` remains `pub(crate)`, no `pub mod` or `pub use` exposes the module or guard, and the only production import is `api`'s private `writer_lock::WriterLock`. All three acquisition sites still call the same `DuckDbDestination::acquire_writer_lock` seam. Repository source search found no stale `mod planning` or `planning::WriterLock` reference.
- Current naming rationale is coherent across this child and `.10x/tickets/done/2026-08-01-destination-module-boundaries.md`: all three destination crates use plural `corrections.rs` and leaf `models.rs`; DuckDB alone has a private `writer_lock.rs`; Postgres alone retains the semantically accurate public-model `plan.rs`. Earlier contrary statements remain dated historical observations followed by explicit correction rather than active guidance.
- All eleven crate docs remain present and accurate against their current manifests and implementations. `cdf-bench-core` owns shared measurement/report contracts, host probes, and child runners; `cdf-benchmarks` owns suites, fixtures, reference comparisons, and report/envelope orchestration while delegating production execution; Arrow IPC exposes file and stream drivers implementing detection, discovery, decode-unit planning, and decoding over runtime byte-source/session contracts; and each of Brotli, bzip2, character, gzip, LZ4, Snappy, xz, and zstd implements the stated streaming transform and named working-set, framing/BOM/checksum, window, or expansion bounds. No repair hunk touches these docs or their authorities.
- Ten documented roots still differ from `HEAD` only by their two-line crate-doc addition. `cdf-benchmarks` also retains the separately owned explicit-facade diff recorded by the core-facade ticket; exact hunk inspection attributes only its first two lines to this ticket. No scoped manifest or dependency change accompanies the documentation.

#### Verdict

**Concerns.** The naming repair itself passes: `writer_lock.rs` is ownership-accurate, implementation-identical after its import block, private outside the crate, fully referenced under its new name, and free of API, behavior, or documentation regression. All eleven crate docs remain accurate. The sole concern is the stale authorization-required entry in the active parent program; reconcile that record before aggregate closure. This ticket remains active as requested.

#### Residual Risk

- This review deliberately did not rerun tests, Rustdoc, formatting, check, or Clippy. It relies on the executor's already-journaled successful commands for compiled validation and independently establishes only source/body/reference/documentation parity.
- The shared uncommitted worktree has no commit boundary between the earlier explicit-import cleanup and this rename. Body comparison, current call-graph inspection, and the two owning tickets make attribution strong, but cannot recreate a commit-isolated rename patch.
- Public compatibility is supported by unchanged private visibility, unchanged explicit root exports, and unchanged call sites; no external semver/API-diff tool was run.

### Final naming-repair closure re-review — 2026-08-01

This final non-authoring pass was deliberately limited to the prior record concern and drift detection against the already-reviewed source state. The delegated review rules were resolved again for this child and its parent. No code was changed and no test, build, lint, formatting, or documentation command was rerun.

#### Findings

None. The parent program's `Blockers` section now states that no semantic or repair-authorization blocker remains and that aggregate quality gates are in progress. This directly resolves the previous minor finding; authorization remains recorded in the parent assumptions and journal, and the completed rename is no longer presented as pending work. The existing source review remains current: `writer_lock.rs` still contains the same private `WriterLock` implementation, the body comparison against former `planning.rs` remains empty, and the only module-path references remain private `mod writer_lock` plus `writer_lock::WriterLock`.

#### Verdict

**Pass.** The sole concern from the naming-repair re-review is closed. The ownership-accurate private rename, body/API/behavior parity, current rationale, and accuracy of all eleven crate docs stand without a remaining finding. This ticket remains active as requested for parent orchestration.

#### Residual Risk

- No validation was rerun in this final pass. The executor's recorded Rustdoc, doctest, check, Clippy, formatting, and hygiene results remain the compiled evidence; this pass establishes only that the reviewed source and record conditions did not drift.
- The shared uncommitted-worktree attribution and absence of an external semver/API-diff run remain unchanged limits from the preceding review. Neither challenges the pass because the repaired module and guard are private and their implementation/call sites remain unchanged.

## Retrospective

- The manifest descriptions were a reliable seed, but an authority boundary needs one additional exclusion to prevent the docs from implying ownership of adjacent runtime concerns. Short “owns X, not Y” wording made the transform and format seams precise without turning crate roots into essays.
- Uniform filenames are useful only for equivalent roles. Inspecting the implementations avoided a cosmetic `plan`/`planning` rename that would have made distinct value-model and lifecycle responsibilities look artificially identical.
- Checking the scoped diff before editing was important in the shared worktree: it identified concurrent explicit-facade work in `cdf-benchmarks`, allowing the documentation-only patch to preserve it exactly.
- Distinguishing non-equivalent modules was necessary but not sufficient: once DuckDB's file proved to own one concrete guard, naming it after that guard (`writer_lock`) is more navigable than retaining the generic lifecycle label `planning`.
