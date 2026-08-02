Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md

# Decompose the engine execution hotspot

## Scope

Refactor `crates/cdf-engine/src/execution.rs`, especially `execute_to_package_inner`, into focused compiler-visible modules for partition lifecycle, schema admission, retry/frontier handling, segment/package sinks, and finalization. Preserve the existing public entry points and observable behavior.

## Non-goals

- No execution-semantic, scheduling, retry, memory, package-identity, telemetry, or performance-default change.
- No public API removal or rename.
- No test weakening or golden regeneration caused solely by refactoring.

## Acceptance Criteria

- `execute_to_package_inner` no longer spans the complete source-open through package-finalization lifecycle; orchestration delegates to focused typed components with explicit imports.
- The resulting module graph is acyclic and contains no production parent/child wildcard imports.
- Existing public execution and preview entry points retain their signatures and behavior.
- Engine determinism, package identity, retry/frontier, schema-admission, drain, and isolated-worker tests pass without weakened assertions.
- Rust complexity evidence shows a material reduction from the audited 2,250-line, cyclomatic-448/cognitive-271 hotspot and records the limits of that metric.

## References

- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/specs/architecture-layering-runtime.md`
- `crates/cdf-engine/src/execution.rs`

## Assumptions

- Record-backed: engine owns execution but must not redefine run meaning.
- User-ratified: behavior-preserving decomposition of the audited hotspot is authorized.

## Journal

- 2026-08-01: Ticket opened from the repository-wide audit; current hotspot begins at `execution.rs:3513` and ends near line 5762.
- 2026-08-01: Execution began. Read the ticket, parent coordination record, all referenced knowledge/specification records, and the repository instructions. `graphify query` was attempted as required, but the `graphify` executable is not installed; source inspection is the authority for this refactor.
- 2026-08-01: Replaced the monolithic file boundary with an explicit `execution.rs` facade and sibling modules for finalization, measurements, orchestration, partition lifecycle, retry/frontier policy, schema admission, and segment handoff. Public exports remain enumerated at the facade.
- 2026-08-01: Extracted invocation validation, package publication, execution-metric ownership, schema-plan admission/disposition, source retry/frontier bounds, partition-open models, durable-segment handoff, partition-completion reconciliation, drain-frontier settlement, and package-artifact preparation. The source/batch loop retains its established ordering and cleanup code, while `execute_to_package_inner` no longer owns final package publication or the extracted post-partition phases.
- 2026-08-01: A concurrent runtime-boundary edit temporarily prevented one intermediate engine check, and a concurrent destination edit temporarily prevented one workspace format check. Both resolved without touching this ticket's files; final ticket-scoped and workspace-format gates are green.
- 2026-08-01: Closure repair authorized after independent review. Repair scope is limited to restoring the pre-refactor residual-decision output lifetime across both finalization hooks and restoring the original elapsed-time overflow authority/text through one shared helper; no broader decomposition or behavior change is authorized.
- 2026-08-01: Repair implemented. `prepare_package_artifacts` now returns its finished `ResidualDecisionOutput` as part of `PreparedPackageArtifacts`; `execute_to_package_inner` retains that owner in its outer lexical scope until `PackageFinalization::finish` returns. A managed residual test now observes the lease, spill reservation, and scratch directory from both hooks and their release only after finalization. The elapsed conversion moved to `measurements.rs`; orchestration and finalization import the one helper with the original `Internal` error mapping and message.
- 2026-08-01: Repair verification completed. The exact regression test, strict all-target Clippy, crate formatting, and all other runnable engine library tests pass. The unfiltered engine suite's sole failure is the independent runtime-ownership source scanner detecting `futures_executor::block_on` in five concurrent `cdf-project/src/runtime_tests/*.rs` layout files; the same suite passes with only that scanner filtered. No project-test file was changed by this repair.

## Blockers

None.

## Evidence

- Acceptance criterion 1 — compiler-visible decomposition:
  - `crates/cdf-engine/src/execution.rs` is a 26-line facade declaring seven explicit child modules and enumerating its public exports.
  - `execution/orchestration.rs` imports each child explicitly. `PackageFinalization::finish`, `validate_execution_invocation`, `reconcile_partition_completion`, `settle_partition_frontier`, and `prepare_package_artifacts` are typed phase boundaries; package publication no longer occurs inside `execute_to_package_inner`.
- Acceptance criterion 2 — acyclic explicit module graph:
  - `rg -n '^use super::' crates/cdf-engine/src/execution/*.rs` shows `orchestration -> {finalization, measurements, partition_lifecycle, retry_frontier, schema_admission, segment_sink}` and `finalization -> measurements`; no child imports orchestration and there is no reverse edge.
  - `rg -n '(^|[^:])\b(use|pub use)\b[^;]*::\*|\b(use|pub use)\s+(crate|super|self)::\*' crates/cdf-engine/src/execution.rs crates/cdf-engine/src/execution --glob '*.rs'` produced no matches.
- Acceptance criterion 3 — public API preservation:
  - A sorted diff between top-level public `fn`/`struct`/`type` declarations in `HEAD:crates/cdf-engine/src/execution.rs` and the final execution facade/module tree produced no output. The public hook/payload types moved behind explicit re-exports; entry-point source was moved without signature edits.
  - `cargo clippy -p cdf-engine --all-targets --locked -- -D warnings`: passed after the final module move, compiling library and test surfaces.
- Acceptance criterion 4 — behavior and protective tests:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine --lib --locked -j 12`: passed after the final source layout, 212 passed, 0 failed, 7 ignored. The passing set includes jobs/determinism, package identity, source retry and reattestation, canonical frontier/drain, schema admission/quarantine, segment cleanup, and isolated-worker memory/authority tests. The seven ignored tests remain pre-existing opt-in performance, stress, and RSS probes; no assertion, golden, or ignore annotation changed.
  - `cargo fmt --all -- --check`: passed.
  - `git diff --check -- crates/cdf-engine/src/execution.rs` and a trailing-whitespace scan over the new execution tree: passed/no matches. Aggregate workspace diff checks remain parent integration authority because new sibling tickets share the worktree.
- Acceptance criterion 5 — complexity reduction:
  - Final `rust-code-analysis-cli -m -O json -p crates/cdf-engine/src/execution/orchestration.rs` reports `execute_to_package_inner` at 1,745 SLOC, cyclomatic 314, cognitive 172, down from the audited 2,250 / 448 / 271 (22.4%, 29.9%, and 36.5% reductions respectively).
  - Extracted units remain bounded relative to the original hotspot: `reconcile_partition_completion` 241 SLOC / 48 cyclomatic / 48 cognitive; `settle_partition_frontier` 117 / 23 / 18; `prepare_package_artifacts` 219 / 47 / 17.
  - Limit: these are syntax-derived maintainability metrics. They demonstrate control-flow distribution, not semantic equivalence; compile, strict lint, and the unchanged 212-test engine suite provide the behavioral evidence.
- Required graph maintenance:
  - `graphify update .` was attempted after the source changes and failed with `command not found`, matching the initial query failure. No graph output was fabricated or edited.
- Closure-repair evidence:
  - `cargo test -p cdf-engine --lib --locked tests::schema_admission::residual_multi_partition_decisions_share_verified_effective_schema_and_keep_identity -- --exact --nocapture`: passed, 1 passed. The assertion observes at least the fixed 8 MiB residual-sort lease, nonzero spill bytes, and the `.residual-decisions-spill` directory from `stream_finalize` and then `pre_finalize`; after execution it observes zero managed/spill bytes and the removed scratch directory.
  - `cargo test -p cdf-engine --lib --locked -j 12 -- --skip standalone_host::tests::production_runtime_ownership_is_centralized`: passed, 211 passed, 0 failed, 7 ignored, 1 filtered out. The unfiltered run reached 211 passed, 1 failed, 7 ignored; its only failure named five concurrent `cdf-project/src/runtime_tests/*.rs` files containing `futures_executor::block_on`, outside this repair's files and semantics.
  - `cargo clippy -p cdf-engine --all-targets --locked -- -D warnings`: passed. `cargo fmt -p cdf-engine -- --check`: passed.
  - `rg -n "fn elapsed_ns|duration exceeds u64 nanoseconds|duration overflow" crates/cdf-engine/src/execution` reports exactly one helper in `execution/measurements.rs`, exactly the original `"{label} duration overflow: {error}"` mapping, and no forked replacement text. The helper still constructs `CdfError::internal`, preserving the original kind and provenance boundary.
  - `graphify update .` was re-attempted after the repair and again failed with `command not found`; this remains a tooling availability limit, not graph-maintenance evidence.

## Review

### Findings

- `significant` — The artifact-preparation extraction changes the lifetime of managed residual-decision scratch across the public finalization hooks. `prepare_package_artifacts` takes `ResidualDecisionAccumulator` by value (`crates/cdf-engine/src/execution/orchestration.rs:5270`), turns it into `ResidualDecisionOutput` (`crates/cdf-engine/src/execution/orchestration.rs:5409`), and drops that output when the helper returns (`crates/cdf-engine/src/execution/orchestration.rs:5470`). Only afterward does `PackageFinalization::finish` invoke `stream_finalize` and `pre_finalize` (`crates/cdf-engine/src/execution/finalization.rs:58-80`). In the pre-refactor `HEAD`, the output was created at `crates/cdf-engine/src/execution.rs:5647` and remained in the outer function scope while both hooks ran at lines 5708-5727. This is observable memory and cleanup behavior: a spill-backed output owns `ResidualDecisionRuns`, which retains the managed memory lease (`crates/cdf-engine/src/residual_spill.rs:22-31,46-65`) and releases the lease plus scratch directory in `Drop` (`crates/cdf-engine/src/residual_spill.rs:132-136`). The extraction therefore releases up to the fixed 8 MiB residual-sort reservation and removes `.residual-decisions-spill` before the hooks instead of after package finalization, contrary to the ticket's explicit no-memory-behavior-change non-goal. Existing residual/spill tests assert cleanup only after execution returns (for example `crates/cdf-engine/src/tests/package_evidence.rs:1287-1292`), so they cannot detect the reordered boundary. Preserve the old lifetime by carrying the residual cleanup owner through `PackageFinalization`, or explicitly ratify and protect the new hook-time memory contract before closing this behavior-preserving ticket.
- `minor` — Finalization forks the timing conversion instead of preserving the existing helper exactly. The new `elapsed_ns` maps overflow to `"{label} duration exceeds u64 nanoseconds"` (`crates/cdf-engine/src/execution/finalization.rs:117-123`), while the retained/original helper maps it to `"{label} duration overflow: {error}"` (`crates/cdf-engine/src/execution/orchestration.rs:7792-7798`; pre-refactor `HEAD:crates/cdf-engine/src/execution.rs:8248-8254`). The branch is practically unreachable on ordinary hardware, but it is still an observable error-shape change and leaves two implementations of one conversion in the new module tree. Move the shared conversion to a neutral child such as `measurements`, or preserve the exact prior mapping.

### Verdict

`concerns`. The principal control-flow fences otherwise survive the extraction: segment failures cancel and join the source frontier before segment cleanup (`crates/cdf-engine/src/execution/orchestration.rs:4797-4811`), empty drains join and abort before deleting the unpublished package (`crates/cdf-engine/src/execution/orchestration.rs:4814-4827`), early drain closure joins the remaining source frontier (`crates/cdf-engine/src/execution/orchestration.rs:4829-4832`), retry completion is reattested before evidence is committed (`crates/cdf-engine/src/execution/orchestration.rs:5049-5085`), and drain settlement maps each former `break`/`continue` branch explicitly to a boolean result (`crates/cdf-engine/src/execution/orchestration.rs:4920-5011`). The facade enumerates exports and the production child graph has only `orchestration -> children` plus `finalization -> measurements`, with no wildcard imports. Protective assertions cover canonical parallel/serial identity (`crates/cdf-engine/src/tests/determinism.rs:14-100,299-360`), segment-frontier abort/no-finalization/memory release (`crates/cdf-engine/src/tests/segmentation.rs:630-679`), partial-retry reattestation and generation drift (`crates/cdf-engine/src/tests/retry_drain.rs:1590-1626,1756-1799`), and drain settlement gating (`crates/cdf-engine/src/tests/retry_drain.rs:32-151`). These observations do not clear the two behavior-preservation findings above.

### Residual Risk

- `execute_to_package_inner` remains a 1,745-SLOC orchestration loop after the recorded reduction, so this review can establish equivalence only for the extracted seams and the assertions inspected; the unchanged batch-loop internals retain substantial inherent complexity.
- No existing assertion observes residual-spill lease/scratch lifetime from `stream_finalize` or `pre_finalize`, and an elapsed-nanosecond overflow is not practically testable. Closure therefore requires source-level restoration (or an explicit contract change), not reliance on the already-passing suite alone.
- This was an independent read-only red-team pass. It used `ocr delegate preview` and scoped `ocr delegate rule`, inspected the pre-refactor `HEAD`, current module tree, and test assertions, and did not repeat the executor's recorded test/lint runs or modify production code.

## Retrospective

- The risky seam was not public API movement but preservation of cancellation, source-frontier join, segment-queue abort, and drain settlement ordering. Keeping the batch loop intact and extracting completed phases around it made those fences auditable while still materially reducing the hotspot.
- Moving the file changed one test-only `include_str!` relative path. Strict all-target Clippy exposed it immediately; the assertion now inspects `orchestration.rs`, the production file it is intended to guard.
- `rust-code-analysis-cli` attributes nested async control flow to the containing function. Extracting final publication alone barely moved the metric; named partition-reconciliation, drain-settlement, and artifact-preparation phases produced the meaningful reduction and also made their ownership explicit.
- Shared-worktree failures should be classified before repair. The transient runtime and destination failures belonged to concurrent children and disappeared when those owners completed their edits; this executor did not overwrite them.
- No reusable new procedure or product semantic was discovered. The durable lesson is already captured by the active compiler-visible module-boundary invariant: phase names and files are insufficient unless control flow and imports actually cross the boundary.
- RAII ownership is part of an extraction's observable ordering even after its logical stream is exhausted. Moving a helper boundary must preserve the old lexical lifetime of cleanup owners across callbacks; the managed residual hook assertion now makes that ordering executable rather than implicit.

## Repair Re-review — 2026-08-01

Fresh independent repair review used `ocr delegate preview` and the engine/memory rule group from
`ocr delegate rule` for `execution/orchestration.rs`, `execution/finalization.rs`,
`execution/measurements.rs`, and `tests/schema_admission.rs`. The review compared the repaired
source with the pre-refactor `HEAD` implementation and did not repeat the executor's recorded test
or lint runs.

### Findings

- None. The prior residual-lifetime finding is closed. `prepare_package_artifacts` finishes and
  returns the `ResidualDecisionOutput` owner (`orchestration.rs:5412-5420,5473-5479`), and the
  caller binds it as `_residual_decision_output` in the outer execution scope
  (`orchestration.rs:4835-4841`). That binding remains live while `PackageFinalization::finish`
  invokes `stream_finalize` and then `pre_finalize` (`finalization.rs:56-78`) and is dropped only
  after `finish` returns or propagates an error. The spill variant retains `ResidualDecisionReader`,
  whose `_runs` field owns the spill reservation, fixed managed-memory lease, and scratch root;
  `ResidualDecisionRuns::drop` releases the lease and removes the directory
  (`residual_spill.rs:22-31,132-136`). The regression assertion observes nonzero spill, at least the
  fixed 8 MiB lease, and the scratch directory inside both hooks, then zero current spill/memory,
  removed scratch, and exact hook order after execution (`tests/schema_admission.rs:1194-1244`).
- None. The prior elapsed-error finding is closed. There is exactly one `elapsed_ns` definition in
  the execution tree, privately owned by `measurements.rs:5-10`; orchestration and finalization
  both import it from that neutral child. Its `CdfError::internal` mapping and
  `"{label} duration overflow: {error}"` text exactly match the pre-refactor helper, and the rejected
  `"duration exceeds u64 nanoseconds"` fork is absent.
- No repair-introduced semantic, public-API, or import defect was found. The retained cleanup owner
  and shared helper are private implementation details, the execution facade's explicit public
  re-exports are unchanged, and the repaired edges remain one-way (`orchestration -> finalization`,
  `orchestration -> measurements`, `finalization -> measurements`) with no reverse dependency.

### Verdict

`pass`. Both prior findings are closed without changing hook order, cleanup timing, error kind/text,
public signatures, or the explicit execution-module ownership graph.

### Residual Risk

- The managed residual regression executes the successful two-hook path. Cleanup when a hook
  returns an error or panics is supported by the same outer lexical binding and RAII `Drop` path,
  but is not separately asserted by this repair test.
- Nanosecond overflow remains impractical to induce in a test; preservation is established by the
  single-helper source comparison rather than runtime execution of that branch.
- The broader 1,745-SLOC orchestration loop retains the inherent complexity recorded by the prior
  review. This repair neither expands nor resolves that pre-existing residual risk.
