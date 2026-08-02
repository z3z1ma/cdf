Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md
Depends-On: .10x/tickets/done/2026-08-01-product-runtime-module-boundaries.md

# Split the project white-box test monoliths

## Scope

Replace `cdf-project/src/tests.rs` and `cdf-project/src/runtime_tests.rs` with focused test module trees organized by project parsing/files, discovery/schema, promotion, publication/recovery, orchestration, destination/replay, and live adapter fixtures.

## Non-goals

- No project behavior, live-test environment policy, fixture semantics, assertion, or ignored-test change.
- No movement of concrete test adapters into production dependencies.

## Acceptance Criteria

- Focused modules replace the 6,141-line and 10,400-line catch-all files.
- Test-only concrete adapter ownership remains behind `cfg(test)`.
- Test names, count, ignored/environment-gated status, fixtures, and assertions are preserved.
- Project tests and strict lint pass.

## References

- `.10x/knowledge/rust-crate-organization.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/knowledge/project-file-publication-recovery.md`

## Assumptions

- Record-backed: concrete destination dependencies in the project manifest are dev-only and support white-box integration tests.

## Journal

- 2026-08-01: Ticket opened from the project test layout finding; it follows the product/runtime boundary cleanup to avoid module-root conflicts.
- 2026-08-01: Execution assigned after the dependency completed. Read the ticket, dependency ticket, and all three governing references completely. Scope is test ownership/layout only: project behavior, live-environment gates, fixtures, assertions, adapter dependency classification, and ignored states remain authoritative.
- 2026-08-01: Attempted the repository-required graph query, but the `graphify` executable is not installed (`zsh: command not found: graphify`). Inspected the source and active records directly.
- 2026-08-01: Reconciled the ticket snapshot with the completed dependency: the current roots were 6,145 and 10,402 lines rather than 6,141 and 10,400. Captured 73 project tests and 101 runtime tests, zero `#[ignore]` attributes, the existing Unix-only schema-symlink test gate, and the `TEST_DATABASE_URL`/local-Postgres fallback gate before moving any item.
- 2026-08-01: Attempted a pre-split `cargo test -- --list`, but the Rust test target could not link because this machine has no discoverable `libduckdb`. This matches the dependency ticket's recorded environment limit, so source-item inventories and hashes are the count/parity authority in this execution.
- 2026-08-01: Split `tests.rs` into `project_files`, `discovery_schema`, `promotion`, `publication_recovery`, and a single shared `support` owner. Split `runtime_tests.rs` into `orchestration`, `promotion`, `destination_replay`, `live_adapters`, and a single shared `support` owner.
- 2026-08-01: Moved every test as a complete attribute-plus-function item and every fixture/helper as a complete non-test item. Added only `pub(super)` visibility required for sibling white-box modules. Concrete DuckDB, Parquet, and Postgres adapter imports remain inside modules declared under `cfg(test)`, and their manifest entries remain dev-dependencies.
- 2026-08-01: The first all-target check exposed one location-relative fixture: `include_str!("../Cargo.toml")` in the moved normal-build-graph test. Reconciled it to `include_str!("../../Cargo.toml")`, which names the identical crate manifest from the focused child module; no assertion or fixture content changed.
- 2026-08-01: Refined four mechanically assigned cases to their clearer owners (HTTP multi-file discovery and destination-bind/quarantine/REST-adapter cases), then repeated formatted hash parity and final all-target compilation, strict lint, formatting, and patch-hygiene checks.
- 2026-08-01: Attempted the final project test run. Rust compilation completed, but native linking again failed on missing `-lduckdb` and DuckDB symbols before any test could execute. Attempted `graphify update .` after the source moves; the executable remains unavailable.
- 2026-08-01: Closure repair dissolved the two support preludes. Every focused case file now imports named parent items and a named subset of shared support; neither support owner reexports its parent with a wildcard, and no `use super::support::*` remains.
- 2026-08-01: Moved every single-scenario fixture family beside its case module while keeping transitive multi-scenario primitives shared. Project support fell from 1,865 to 307 lines and now exposes 10 genuinely shared items; runtime support fell from 4,424 to 2,006 lines and now exposes 64 shared items. The retained runtime `mock_bulk_path` helper is consumed by the shared `MockProjectDestinationRuntime`, which is exercised by destination/replay and live-adapter scenarios even though the helper also has one direct destination/replay call.
- 2026-08-01: Compared the final tree to the pre-repair snapshot after formatting. All 63 project-support and 138 runtime-support exported fixture items retain exact text and count. All 75 current project test items and 101 runtime test items retain exact attribute-plus-body suffixes; the 75 project tests comprise the ticket's original 73 plus the dependency ticket's two architecture regression tests, so all original 174 scenarios and both later checks are preserved.
- 2026-08-01: Ticket-scoped all-target compilation, strict Clippy, formatting, and patch-hygiene checks pass. Per orchestration scope, no runtime or privileged project test was started in this closure repair; the ticket remains active for fresh independent review and the separately authorized execution gate.
- 2026-08-01: Parent integration ran the full project library suite outside sandbox restrictions with `DUCKDB_DOWNLOAD_LIB=1`. All 270 tests passed with no failures or ignores, including the 176 current project/runtime layout tests, both product-boundary architecture tests, DuckDB-backed paths, and live PostgreSQL cases.
- 2026-08-01: Aggregate conformance execution exposed a finite-boundary false positive after the project test split: `generic_project_and_cli_runtime_sources_do_not_import_destination_crates` allows only root filenames, so it scans the test-owned `cdf-project/src/tests/project_files.rs` and rejects its concrete destination test imports. The authorized repair is limited to the conformance guard: preserve the exact production-file allowlists, add exact normalized root-relative `tests/` and `runtime_tests/` exclusions, and add a same-prefix/nested-filename counterexample proving no broader production path is skipped.
- 2026-08-01: Reconciled the 17 remaining `cdf-project` coverage-registry references in `cdf-conformance/src/run_matrix/data_onramp.rs` from the removed `tests.rs`/`runtime_tests.rs` monoliths to the unique focused test file containing each unchanged function. Concurrent CLI mappings were preserved; project function order and registry membership are unchanged. Per scope, no Cargo command or rustfmt was run.

## Blockers

None.

## Evidence

- Layout: `tests.rs` is now a 65-line import/module facade. Its focused files contain 20 project-file, 44 discovery/schema, 2 promotion, and 7 publication/recovery tests; shared support is in `tests/support.rs`. `runtime_tests.rs` is now a 92-line facade. Its focused files contain 35 orchestration, 5 promotion, 37 destination/replay, and 24 live-adapter tests; shared support is in `runtime_tests/support.rs`. The totals remain exactly 73 and 101.
- Test/assertion parity: both pre-split roots and all post-split files were formatted, then an item-level script keyed each test by leaf identifier and SHA-256 hashed its complete attributes and function text. Runtime result: 101 before/101 after, exact equality, no missing/added/changed names. Project result: 73/73 with no missing or added names and exactly one changed item, `project_normal_build_graph_has_no_concrete_destination_crates`; normalizing only `include_str!("../../Cargo.toml")` back to its old location-relative spelling produced exact 73/73 equality. This proves every other attribute, fixture call, body, and assertion is textually identical and the sole reconciliation still embeds the same `cdf-project/Cargo.toml` bytes.
- Gate parity: both roots had and retain zero `#[ignore]` attributes. The complete `#[cfg(unix)] #[test]` item for `schema_snapshot_store_rejects_managed_ancestor_and_leaf_symlinks` hashes identically before/after. `LivePostgres::start` retains the exact `TEST_DATABASE_URL` lookup, empty-value handling, local `postgres`/`initdb`/`pg_ctl` fallback, and skip message; its only source-level changes are sibling-test visibility qualifiers on the moved fixture owner.
- Test-only adapter boundary: `cdf-dest-duckdb`, `cdf-dest-parquet`, and `cdf-dest-postgres` remain under `[dev-dependencies]`; `runtime_tests` and `test_destinations` remain declared under `#[cfg(test)]`. `cargo tree -p cdf-project -e normal --locked --prefix none` exited 0 with zero concrete destination matches. This proves the normal dependency graph did not acquire the test adapters.
- Compilation: final `cargo check -p cdf-project --all-targets --locked` passed, including both complete test module trees.
- Strict lint: final `cargo clippy -p cdf-project --all-targets --locked --no-deps -- -D warnings` passed.
- Formatting and patch hygiene: `cargo fmt -p cdf-project -- --check` passed. Scoped `git diff --check` for both test trees and this ticket passed after the completed evidence update.
- Test execution limit: both pre-split `cargo test -p cdf-project --lib --locked -- --list` and final `cargo test -p cdf-project --lib --locked -j 12` compiled Rust successfully but failed during native linking with `library not found for -lduckdb` and unresolved DuckDB symbols. Consequently no project test was executed or harness-enumerated in this environment. The passing all-target check and strict all-target Clippy prove the final Rust test targets compile; hashes and source counts prove preservation within their stated limits.
- Graph freshness limit: `graphify update .` failed with `zsh: command not found: graphify`. No claim is made that `graphify-out/` contains the new project test paths.
- Closure-repair ownership scan: `tests/support.rs` is 307 lines with 10 retained exports; `runtime_tests/support.rs` is 2,006 lines with 64 retained exports. A case-consumer and intra-support reference scan found no project support item with fewer than two transitive case owners. Every runtime item likewise has multiple transitive owners; `mock_bulk_path` is reached through the shared `MockProjectDestinationRuntime` implementation in addition to its direct destination/replay use. No support wildcard import or parent wildcard reexport remains.
- Closure-repair parity: a saved-source comparison against `/private/tmp/cdf-project-test-layout-before` found project tests 75/75 exact and runtime tests 101/101 exact. It found project support exports 63/63 exact (`SHA-256 c7a1d4ea101797966145cfdc7d8011731945d72eb9c5b73772df2dbe31794282`) and runtime support exports 138/138 exact (`SHA-256 9f08c1188ec361a9aba5ed75c676b8d61d7d93380538cc94bebddb9b1d8b7d9b`). Limit: these hashes prove source preservation after relocation, not runtime behavior beyond the preserved assertions.
- Closure-repair compile: `cargo check -p cdf-project --all-targets --locked --message-format short` — passed with no warnings.
- Closure-repair strict lint: `cargo clippy -p cdf-project --all-targets --locked --no-deps -- -D warnings` — passed.
- Closure-repair formatting and patch hygiene: `cargo fmt -p cdf-project -- --check` and scoped `git diff --check` — passed.
- Closure runtime evidence: `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib --locked -j 12` completed outside sandbox restrictions: 270 passed, 0 failed, 0 ignored. This closes the test-execution criterion for the preserved project/runtime test trees and their environment-backed paths.
- Coverage-registry path parity: an exact source scan found 17 current `cdf-project` registry references; every function name occurs in exactly one focused file and each registry path names that file. Comparing the pre-split and current reference streams found identical function names, order, and membership. The stale-monolith scan for `crates/cdf-project/src/tests.rs::` and `runtime_tests.rs::` returned zero matches. This is source-level registry evidence only; no compilation or runtime claim is added.

## Review

- 2026-08-01 independent red-team review used `open-code-review-delegate` to resolve the workspace test-layout files and product-boundary rules, then inspected both test trees, their original `HEAD` roots, the manifest/test gates, and the recorded environment limit without repeating the executor's Cargo runs.
- Findings:
  - **significant** — both `support.rs` files are replacement catch-alls and hidden test preludes, not owners limited to genuinely shared fixtures. `crates/cdf-project/src/tests/support.rs` is 1,865 lines and `runtime_tests/support.rs` is 4,424 lines; each starts with `pub(super) use super::*;`, and every focused case file imports the complete surface with `use super::support::*;` at line 1. Of 63 named `pub(super)` exports in project support, 44 are referenced directly by exactly one case module; of 138 in runtime support, 76 are referenced directly by exactly one case module. Clear single-owner families include the project-files safety inventory (`tests/support.rs:3-193`), discovery/schema fixtures (`:201-918`, `:980-1865`), publication-only lease fixtures (`:920-978`), orchestration-only drain/tracing/state fixtures (`runtime_tests/support.rs:305-1479`, `:3203-3794`), destination/replay package and mock-runtime fixtures (`:152-304`, `:1610-3113`, `:4005-4380`), and live-adapter REST/Postgres fixtures (`:1498-1609`, `:3114-3315`, `:3795-4004`, `:4104-4268`). The eight case modules themselves have coherent names and test ownership, but the broad re-export/import chain hides their actual fixture dependencies and relocates most scenario setup into two new monoliths. Move single-module fixture families beside their cases, retain only multi-module primitives in support, and use explicit support imports.
  - **significant** — the recorded native-link failure is a misclassified command-configuration failure, so the acceptance criterion that project tests pass is unmet. `.10x/knowledge/developer-build-duckdb-linkage.md:27-65` requires routine local Cargo builds involving DuckDB to set `DUCKDB_DOWNLOAD_LIB=1`; it activates the pinned prebuilt native-library path, and the repository slow-quality workflow sets the same variable globally (`.github/workflows/slow-quality.yml:16-18`). The ticket attempted plain `cargo test` before and after the split, then concluded that this environment cannot execute the tests because `-lduckdb` is unavailable. Those attempts prove only that the required developer link input was omitted; they do not establish an environmental impossibility. Consequently none of the 174 moved tests was executed or harness-enumerated, despite an explicit test-pass acceptance criterion. Repair requires running the pre/post-equivalent inventory or at least the final suite with the documented `DUCKDB_DOWNLOAD_LIB=1` mode (requesting network approval if its target cache must be populated) and recording the real result.
- Falsification evidence:
  - An independent brace-aware comparison keyed complete attribute-plus-`#[test] fn` items from `HEAD:crates/cdf-project/src/tests.rs` and `runtime_tests.rs` against the eight case files. Runtime parity is exact at 101/101 with no missing, added, or changed item. Project parity is 73/73 with only `project_normal_build_graph_has_no_concrete_destination_crates` changed; normalizing its new `include_str!("../../Cargo.toml")` spelling to the former `include_str!("../Cargo.toml")` makes all 73 items byte-identical.
  - The old and new `include_str!` operands both resolve to `crates/cdf-project/Cargo.toml` and read the same SHA-256 bytes; the assertion still inspects the normal `[dependencies]` table (`tests/project_files.rs:172-187`). The relative-path reconciliation is therefore semantically exact.
  - The whole crate retains 268 `#[test]` attributes and zero `#[ignore]` attributes, matching `HEAD`. All three Unix-gated source occurrences are preserved, including the complete top-level `#[cfg(unix)]` symlink test. Normalized fixture comparison found all 80 project-support and 196 runtime-support top-level items in their former roots after removing only sibling visibility, whitespace/comments, and rustfmt trailing commas. `LivePostgres::start` retains its exact `TEST_DATABASE_URL`, empty-value, local-server fallback, and skip-message behavior (`runtime_tests/support.rs:3882-3922`).
  - Concrete adapter ownership remains test-only: the three destination crates are unchanged under `[dev-dependencies]` (`crates/cdf-project/Cargo.toml:34-46`); `runtime_tests`, `test_destinations`, and `tests` remain gated by `#[cfg(test)]` (`src/lib.rs:76-86`); and concrete destination imports occur only in those gated modules. No manifest change was made, and the recorded normal-only Cargo tree plus the preserved manifest assertion support destination-neutral normal dependency direction.
- Verdict: **fail**. Test/gate/fixture/include-path parity and the concrete-adapter dev boundary hold, and no project behavior regression was found. However, the central layout outcome still contains two large wildcard support catch-alls, and the required test-pass evidence was not obtained because the documented developer DuckDB linkage mode was not attempted.
- Residual risk: no project test executed in this ticket, so source parity proves preservation only within the existing assertions; environment-gated live Postgres cases may still self-skip under their preserved policy; the declared dependency `.10x/tickets/done/2026-08-01-product-runtime-module-boundaries.md` remains active with a failed review, so this ticket's dependency graph is not closure-ready; and `graphify-out/` freshness remains unverified because the executable was unavailable. The ticket remains active for closure judgment and separately authorized repair.

## Support-ownership Repair Re-review — 2026-08-01

### Findings

- None. The former 1,865-line project support catch-all is now 307 lines with 10 exports (an 83.5% reduction), and the former 4,424-line runtime support catch-all is now 2,006 lines with 64 exports (a 54.7% reduction). Neither support owner reexports its parent, and every focused case uses explicit named parent and `support::{...}` imports; searches find no support wildcard, parent wildcard, or case-to-case dependency edge.
- Reapplying a transitive case-owner scan found at least two scenario owners for every retained project and runtime support export. Project support coherently owns shared execution/source registry construction, its registry-private driver/transport components, the cross-case project/resource documents, and destination-sheet construction. Runtime support coherently owns shared run/resource wrappers, package/artifact primitives, destination runtime fixtures, and live adapter foundations. `mock_bulk_path` is the sole item whose direct symbol count names one case; its second scenario owner is real and compiler-visible through `MockProjectDestinationRuntime::runtime_capabilities`, and that shared runtime is consumed by both destination/replay and live-adapter cases.
- Independent inventories against the preserved pre-repair snapshot find exactly 75/75 project test names, 101/101 runtime test names, 63/63 unique project helper names, and 138/138 unique runtime helper names, with empty missing/added sets and identical Unix/ignore gate counts. The executor's complete-item hashes establish exact attribute, body, gate, fixture, and assertion text after relocation; source inspection found no new test or helper edit outside those recorded moves.
- The one historical location-relative reconciliation remains semantically exact: `HEAD`'s `src/tests.rs` operand `../Cargo.toml` and the current `src/tests/project_files.rs` operand `../../Cargo.toml` both resolve to the same `crates/cdf-project/Cargo.toml` file. The test still parses the normal `[dependencies]` table and rejects concrete `cdf-dest-*` entries.
- The adapter boundary remains test-only. DuckDB, Parquet, and Postgres destination crates are unchanged under `[dev-dependencies]`; their Rust imports occur only in `runtime_tests`, `tests`, and `test_destinations`, all declared behind `#[cfg(test)]`. The manifest has no repair diff and the normal dependency table contains no concrete destination crate.

### Verdict

**Pass for the authorized ownership repair.** Both replacement catch-alls are materially dissolved, focused dependencies are explicit and acyclic, retained support has coherent multi-scenario ownership, source/gate/assertion parity holds at the recorded 75+101 test and 63+138 helper inventories, and neither the embedded-manifest check nor the dev-only adapter boundary regressed. The ticket remains active for the separately authorized execution-evidence gate and orchestrator closure judgment.

### Residual Risk

- This review intentionally did not run tests. Complete-item hashes and compilation/lint evidence prove source preservation and type correctness within their stated limits, not runtime behavior; the prior review's missing DuckDB-enabled execution evidence remains a separate closure gate.
- The environment-gated live Postgres cases retain their exact `TEST_DATABASE_URL` and local-server fallback/skip policy through helper-body parity, but may self-skip when neither path is available.
- Runtime shared support remains substantial at 2,006 lines because its retained families cross scenario boundaries. The transitive ownership scan justifies their current location; future consumer removal should rerun that scan so shared support does not silently regrow into a catch-all.

## Final Closure Review — 2026-08-01

### Verdict

**Pass.** Parent integration's DuckDB-enabled full project library run passed 270/270 with 0 failures and 0 ignored, including all 176 current project/runtime layout tests and the live PostgreSQL and DuckDB-backed paths. This supplies the runtime evidence intentionally left open by the support-ownership re-review. The referenced product/runtime boundary repair also has a passing independent verdict and matching full-suite evidence, so its earlier failed-review and execution-limit residuals no longer block this ticket. All acceptance criteria are supported; status remains active for orchestrator closure.

### Residual Risk

- The full-suite result proves the behavior encoded by the current 270 library tests in that authorized execution environment; it does not claim coverage beyond those assertions.
- Runtime shared support remains 2,006 lines by design because its retained fixture families have multiple scenario owners. Future consumer removal should repeat the ownership scan to prevent it from regrowing into a catch-all.

## Aggregate Conformance Repair Review — 2026-08-01

Fresh independent review used the delegated open-code-review rule selection and inspected only the project-layout repairs in `cdf-conformance/src/destination_catalog.rs` and the 17 project registry mappings in `cdf-conformance/src/run_matrix/data_onramp.rs`. Concurrent CLI mappings were excluded from this verdict, and no test was rerun.

### Findings

None.

- The destination-import guard now compares normalized root-relative paths by exact `Path` equality for both allowed files and allowed test roots. A root exemption skips the exact `tests` or `runtime_tests` directory at the traversal boundary, so all nested test-owned files are allowed without broadening the exemption to `tests_production`, `runtime_tests_extra`, or another same-prefix directory. Root-only allowed filenames likewise do not exempt `nested/allowed.rs`.
- The regression fixture falsifies the important overbroad implementations in one deterministic assertion: it places concrete imports beneath both nested allowed test roots, beneath both deceptive same-prefix production roots, in an exact allowed root file, and in a nested file with the same basename. The returned violation list is sorted before comparison and diagnostic rendering, so filesystem `read_dir` order cannot affect the result.
- An independent `HEAD`-to-worktree registry reconstruction found 17 references before and 17 after. The complete `(scenario-or-friction, id, function-name)` streams are identical, proving order and membership preservation. Every current path exists, contains exactly one matching function definition, and is the sole defining focused project/runtime test file for that function; the resolution scan reported zero errors. No `crates/cdf-project/src/tests.rs::` or `runtime_tests.rs::` registry reference remains.

### Verdict

**Pass for the project-layout aggregate conformance repairs.** The allowed boundary is finite and exact, nested test roots remain excluded, same-prefix and same-basename production counterexamples remain scanned, violation traversal is deterministic at the observable boundary, and all 17 project registry occurrences preserve their names, order, membership, and unique focused-file ownership. No project repair defect was found. Status remains active for orchestrator closure judgment.

### Residual Risk

- Per review instruction, this review did not rerun the regression or shared registry guard; it validates their source assertions and the independent path/function inventory. Execution evidence remains owned by the aggregate integration run and exact guard already recorded elsewhere in the workstream.
- The import guard retains its pre-existing textual `cdf_dest_` detection boundary. This repair makes path exemptions exact; it does not claim to detect destination coupling expressed without that token.
- Determinism is established for the returned and rendered violation ordering through the final sort. As before, the filesystem walker assumes an ordinary readable, acyclic source tree; symlink-cycle hardening is outside this layout repair.

## Retrospective

- What worked: complete-item extraction plus formatted per-test hashes made preservation of 174 large white-box scenarios auditable without reviewing a 16,547-line textual move by eye. Keeping a support owner per original root preserved fixture identity and avoided duplicating concrete adapters.
- What surprised: `include_str!` is resolved relative to the physical source file, so a pure module-layout move can require one source spelling change even when fixture bytes remain identical. Normalizing that single known path made the parity exception explicit and falsifiable.
- Durable lesson: test-layout parity should inventory the full attribute-plus-function item, not just function names. That caught and preserved the Unix gate automatically; environment gates housed in fixture methods need a separate before/after inspection because they are outside test items.
- Limit: the unavailable native DuckDB library prevents both baseline enumeration and final execution, so this ticket's behavioral evidence is compiled-test coverage plus exact test-body parity rather than a runtime pass. The failure is unchanged from the completed dependency and is not caused by the layout.
- Follow-up ownership: no product behavior defect, adapter-boundary leak, or new code follow-up was discovered. The missing `graphify` executable and native DuckDB library are environment limitations already visible to the parent audit; they do not justify product-code changes in this ticket.
