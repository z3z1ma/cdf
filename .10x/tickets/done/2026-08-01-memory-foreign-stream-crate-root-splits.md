Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md

# Split memory and foreign-stream contract roots

## Scope

Make `cdf-memory` and `cdf-foreign-stream` thin explicit facades. Split cgroup/budget discovery, leases/accounted payloads, coordinator/waiters, and budget resolution in `cdf-memory`; split descriptors, cancellation, events, control/terminal values, and producer contracts in `cdf-foreign-stream`.

## Non-goals

- No memory accounting, admission, cancellation, foreign stream, schema acquisition, or public contract semantic change.
- No new dependency or executor-specific type.

## Acceptance Criteria

- Both crate roots are thin documented facades with explicit public exports.
- Module imports are explicit and acyclic; shared values have one owner.
- Public API remains stable.
- Memory, cancellation, foreign producer, Python/subprocess integration, and strict lint checks pass.

## References

- `.10x/knowledge/rust-crate-organization.md`
- `.10x/specs/runtime-memory-backpressure.md`
- `.10x/specs/execution-host-structured-runtime.md`

## Assumptions

- Record-backed: both roots contain multiple coherent contract families that can move without semantic change.
- User-ratified: existing public names and behavior remain authoritative.

## Journal

- 2026-08-01: Ticket opened from the crate-root layout audit.
- 2026-08-01: Execution assigned. Read the ticket and all three governing references completely. The authorized change is ownership and facade layout only: existing public names, accounting/admission/release behavior, cancellation propagation, event sequencing, schema acquisition, and producer contracts remain authoritative.
- 2026-08-01: Captured a pre-change rustdoc inventory for both crate roots. It established 36 public root items for `cdf-memory` and 31 for `cdf-foreign-stream`, providing a mechanically comparable API baseline before moving definitions.
- 2026-08-01: Split `cdf-memory` into `cgroup`, `budget`, `accounting`, and `coordinator` owners while retaining the existing `spill` owner. The 41-line crate root now declares modules and explicitly re-exports the existing public surface; no wildcard facade export was introduced.
- 2026-08-01: Split `cdf-foreign-stream` into `descriptor`, `cancellation`, `control`, `events`, and `producer` owners, with tests in `tests.rs`. The 27-line crate root now declares modules and explicitly re-exports the existing public surface.
- 2026-08-01: The first all-target compile exposed test-only reliance on names inherited from the former monolithic roots (`Result`, cgroup parsers, `Stream`, `Batch`, and error/result aliases). Repaired those test seams with explicit imports and narrow test-only helpers; production visibility was not widened.
- 2026-08-01: Expanded the foreign-stream architecture test from inspecting only `lib.rs` to inspecting every Rust source file in the crate so its executor-independence rule continues to protect the split implementation rather than only the facade.
- 2026-08-01: Completed API-parity, focused unit/integration, formatting, and strict-lint validation. `graphify update .` was also attempted as required by repository instructions, but the `graphify` executable is not installed in this environment (`zsh: command not found: graphify`).
- 2026-08-01: Final scoped `git diff --check -- crates/cdf-memory crates/cdf-foreign-stream .10x/tickets/done/2026-08-01-memory-foreign-stream-crate-root-splits.md` exited 0. Execution is complete; the ticket remains active for the required independent review.
- 2026-08-01: Closure repair authorized after independent review. Scope is limited to making the foreign-stream executor-neutrality/non-collection source scan recursively cover production Rust modules while excluding test sources and preserving every existing forbidden-pattern assertion.
- 2026-08-01: Closure repair implemented. `source_text` now obtains a sorted recursive inventory of regular Rust files, descends through nested production directories, excludes `tests.rs` and `tests/` sources, and does not follow non-file/non-directory entries. The executor/runtime and eager-collection assertion loops and their forbidden token sets are unchanged.
- 2026-08-01: Added a focused synthetic layout assertion with root and nested production modules, root/nested test sources, and a non-Rust file. It proves nested production coverage and exact test-source/non-Rust exclusion independently of the crate's current flat layout. Focused guard tests, the full foreign-stream unit suite, strict Clippy, and formatting all pass; `graphify update .` remains unavailable.

## Blockers

None.

## Evidence

- Facade shape: `wc -l crates/cdf-memory/src/lib.rs crates/cdf-foreign-stream/src/lib.rs` reported 41 and 27 lines. `rg -n 'pub use [^;]+::\*'` found no wildcard public exports in either root.
- Ownership and dependency direction: production imports form the intended one-way edges `coordinator -> accounting`, `events -> control + descriptor`, and `producer -> cancellation + descriptor + events`; cgroup, budget, accounting, cancellation, control, and descriptor are leaves. Inspection found no reverse edge or duplicate owner for shared values.
- Public API parity: normalized rustdoc root-item inventories generated before and after the split compared byte-for-byte equal. `cdf-memory` retained all 36 root items and `cdf-foreign-stream` retained all 31 root items. Procedure: `cargo doc -p cdf-memory -p cdf-foreign-stream --no-deps --locked` into separate temporary target directories, normalize each crate's root item names/kinds, then `diff` the before/after inventories; both diffs exited 0. This proves the documented root item surface, not downstream source compatibility beyond what the compilation checks exercise.
- Compilation: `cargo check -p cdf-memory -p cdf-foreign-stream --all-targets --locked` passed. This proves both crates and all of their local targets type-check after the split.
- Focused behavior: `cargo test -p cdf-memory -p cdf-foreign-stream --lib --locked` passed with 25/25 memory tests and 9/9 foreign-stream tests. After the final cancellation test-helper visibility cleanup, `cargo test -p cdf-foreign-stream --lib --locked` again passed 9/9. These runs cover retained-byte accounting, reservations/releases, waiter behavior, cgroup/budget resolution, cancellation, event projection/summary, and the architecture guard within the assertions present.
- Strict lint: `cargo clippy -p cdf-memory -p cdf-foreign-stream --all-targets --locked --no-deps -- -D warnings` passed after the final source state.
- Downstream integration compilation: `cargo check -p cdf-python -p cdf-subprocess --all-targets --locked` passed.
- Downstream integration behavior: `cargo test -p cdf-python -p cdf-subprocess --lib --locked` passed: `cdf-python` 34 passed, 0 failed, 7 ignored; `cdf-subprocess` 30 passed, 0 failed, 1 ignored. Ignored tests were not exercised by this command.
- Formatting and patch hygiene: `cargo fmt -p cdf-memory -p cdf-foreign-stream -- --check` passed. The final scoped `git diff --check` is recorded in the closing journal entry below.
- Graph freshness limit: `graphify update .` could not run because the executable is absent. No claim is made that `graphify-out/` reflects these source moves.
- Closure-repair evidence:
  - `cargo test -p cdf-foreign-stream --lib --locked tests::production_source_scan_recurses_and_excludes_test_sources -- --exact --nocapture`: passed, 1 passed. The fixture's expected inventory contains `lib.rs`, `producer/mod.rs`, and `producer/nested/adapter.rs`, while excluding root/nested `tests.rs`, a nested `tests/` tree, and a non-Rust file.
  - `cargo test -p cdf-foreign-stream --lib --locked tests::crate_contract_stays_executor_neutral_and_non_collecting -- --exact --nocapture`: passed, 1 passed. This exercises every pre-existing forbidden-runtime and forbidden-collection assertion against the recursive production inventory.
  - `cargo test -p cdf-foreign-stream --lib --locked`: passed, 10 passed, 0 failed, 0 ignored.
  - `cargo clippy -p cdf-foreign-stream --all-targets --locked --no-deps -- -D warnings` and `cargo fmt -p cdf-foreign-stream -- --check`: passed.
  - `graphify update .` was re-attempted after the repair and failed with `command not found`; no graph output was fabricated or edited.

## Review

- 2026-08-01 independent red-team review used the repository's `open-code-review-delegate` workflow to resolve the workspace file set and the hostile memory/foreign-runtime rules, then inspected the relevant production and test diff without repeating the executor's recorded verification.
- Findings:
  - **minor** — `crates/cdf-foreign-stream/src/tests.rs:396` describes a crate-wide source scan, but `source_text` uses one non-recursive `std::fs::read_dir(src)` and retains only immediate `.rs` entries (`:398-407`). Every current production module is a top-level file and was therefore covered, but a future conventional nested module such as `src/producer/adapter.rs` would be silently omitted from the executor-neutrality and non-collection assertions. The guard is materially stronger than the former `lib.rs`-only check, but it is not recursively crate-wide.
- Falsification evidence:
  - Public parity held under source inspection: the two private-module facades explicitly re-export the same public owners (`crates/cdf-memory/src/lib.rs:17-35`, `crates/cdf-foreign-stream/src/lib.rs:9-24`), and comparison of each extracted public definition, associated implementation, trait, field, and helper against its pre-split root found only import qualification, equivalent `Poll` aliases, private-module visibility needed for sibling access, and `#[cfg(test)]` accessors. This corroborates the executor's byte-equal 36-item/31-item rustdoc inventories rather than relying on those inventories alone.
  - Memory accounting, lease partition/reconcile/drop, reservation admission, waiter registration/drop, release, cgroup parsing/reporting, and budget-resolution bodies are unchanged from the pre-split root. The only production dependency edge is `coordinator -> accounting`; cgroup, budget, accounting, and spill have no reverse edge. No new manifest dependency was introduced.
  - Foreign cancellation/drop, event projection, control/terminal handling, descriptor validation, and producer-open bodies are unchanged from the pre-split root apart from equivalent imported `Context`/`Poll` names. The production DAG is one-way: `events -> control + descriptor` and `producer -> cancellation + descriptor + events`. Direct inspection found no concrete language runtime, query engine, process launcher, private executor, or eager batch-vector type in production source.
  - Protective behavior assertions were preserved. The cancellation waiter assertions now use a narrow test-only accessor; the memory poison assertion now uses narrow test-only helpers; all other extracted test changes are import/formatting changes except the intended source-scan expansion. The executor's focused and downstream evidence remains authoritative within its stated limits; this review did not rerun it.
- Verdict: **concerns**. No critical or significant defect and no current semantic or public-contract regression was found. The non-recursive architecture scan is a minor durability gap to resolve before calling the guard fully crate-wide.
- Residual risk: ignored downstream Python/subprocess tests were not exercised by the recorded commands; rustdoc inventory equality alone cannot prove every downstream source-compatibility pattern, though unchanged definitions, explicit root re-exports, and downstream compilation substantially reduce that risk; `graphify-out/` freshness remains unverified because the executable was unavailable. The ticket remains active for closure judgment and any separately authorized repair.

## Retrospective

- What worked: capturing the rustdoc inventory before moving definitions made public-surface preservation exact rather than inferential. Focused owner modules also made the cross-module dependency graph small enough to inspect directly.
- What surprised: the monolithic roots had implicitly supplied several names to nested tests. Moving tests behind modules correctly made those hidden dependencies fail compilation; replacing them with explicit imports exposed the real test seams without changing production contracts.
- Durable lesson: architecture tests that scan only a crate root become porous after implementation moves into child modules. Crate-wide source scanning is the appropriate invariant for bans that are meant to apply to the whole crate.
- Closure-repair lesson: a recursive guard is not protected merely because its walker looks recursive while today's tree is flat. A synthetic nested production/test fixture makes traversal depth and exclusion behavior executable before the repository naturally acquires that layout.
- Follow-up ownership: no additional code defect or architectural debt was discovered within this ticket's scope. The only residual limitation is the unavailable local `graphify` executable, recorded above for the orchestrator rather than converted into a product-code ticket.

## Recursive-guard Repair Re-review — 2026-08-01

### Findings

- None. `production_rust_source_paths` recursively visits every real directory below the supplied source root, retains every regular file whose extension is exactly `.rs` except a file named exactly `tests.rs`, prunes only directories named exactly `tests`, and sorts the complete `PathBuf` inventory before returning it. Thus `read_dir` enumeration order cannot affect the scan, nested production modules at arbitrary depth are covered, and similarly named production paths are not excluded.
- The production guard calls that same inventory helper and reads every returned path before applying its assertions. Comparison with the pre-split guard found all seven forbidden-runtime tokens, all three forbidden eager-collection spellings, the manifest/source checks, case normalization, negative assertion polarity, and diagnostic messages unchanged; the repair changes coverage rather than weakening the protected invariant.
- The synthetic traversal test uses the real helper against root production, one-level production, and two-level production Rust files, plus root/nested `tests.rs`, a nested `tests/` tree, and a non-Rust file. Its exact path assertion would fail if traversal regressed to the former root-only scan, stopped after one directory level, admitted either excluded test form, or admitted non-Rust files. `source_text` is directly wired to the tested helper, so future nested production modules enter the actual forbidden-pattern scan; deterministic ordering is enforced directly by the helper's unconditional final sort.
- The repair is entirely inside `src/tests.rs`, which is included only by the crate root's existing `#[cfg(test)] mod tests`; it adds no production module, dependency, export, or public signature. Inspection found no production or API behavior path from the traversal helper.

### Verdict

**Pass.** The prior non-recursive guard concern is resolved. The recursive inventory is deterministic, its exclusions are narrowly exact, the original executor-neutrality and non-collection assertions are intact, and the fixture materially falsifies the traversal regressions it is intended to prevent. The ticket remains active for the orchestrator's closure judgment.

### Residual Risk

- The repository's current production layout is flat, so real-tree depth coverage comes from the synthetic fixture rather than an existing nested production module. The fixture exercises two nested levels with the same helper used by the guard, which is sufficient for the recursive algorithm but does not make filesystem traversal infallible on every platform.
- The walker deliberately ignores symlinks and other non-regular entries instead of following them; this avoids traversal escapes and cycles and matches the repair's recorded regular-file scope. A future decision to compile symlinked Rust modules would require revisiting that policy.
- This review inspected the test logic and relied on the executor's journaled focused/full test, Clippy, and formatting evidence rather than repeating it. `graphify` remains unavailable, so no refreshed graph artifact supports this review.
