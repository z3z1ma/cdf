Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md

# Split the CLI white-box test monolith

## Scope

Replace the 17,654-line `cdf-cli/src/tests.rs` with a focused `tests/` module tree organized by command family and cross-cutting contract such as redaction, reports, progress, recovery, and composition.

## Non-goals

- No CLI grammar, output, error, fixture behavior, or assertion change.
- No conversion to integration tests when white-box access is required.

## Acceptance Criteria

- No focused test module becomes another catch-all; names correspond to command families or cross-cutting contracts.
- Test names, count, ignored status, fixtures, and assertions are preserved.
- CLI unit/integration tests and strict lint pass.

## References

- `.10x/knowledge/rust-crate-organization.md`
- `.10x/specs/project-cli-observability-security.md`

## Assumptions

- Record-backed: the current test file is 17,654 lines and already contains separable command families.

## Journal

- 2026-08-01: Ticket opened from the white-box test layout finding.
- 2026-08-01: Execution started. Read the owning ticket and both referenced active records; constrained the change to the CLI white-box test module map and exact relocation of existing test code.
- 2026-08-01: Moved the shared imports, constants, macros, fixtures, and helpers into `tests/mod.rs`, then relocated the existing tests without assertion edits into 22 focused modules: surface, init/validate, add, contract, planning, schema discovery, schema promotion, source planning, preview, run, inspect, recovery, adapter runs, replay, status, SQL, doctor, Python, doctor drift, package, state, and errors.
- 2026-08-01: The two production-source guard tests formerly excluded the single path `tests.rs`. Updated only their source-selection fixtures to exclude the replacement `tests/` tree; their assertions and production scan rules are unchanged. Both exact guard tests pass after the adaptation.
- 2026-08-01: The full sandboxed CLI library suite ran 300 tests: 269 passed and 31 failed. Two failures were the layout-sensitive guard fixtures above and were repaired. The other 29 originated from sandbox-denied loopback socket/Postgres fixture setup (`Operation not permitted`) or the resulting poisoned Postgres fixture mutex; they did not report assertion regressions in relocated test bodies.
- 2026-08-01: Requested an unsandboxed rerun of `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib --locked` so loopback and temporary Postgres fixtures could execute. The approval/run was interrupted before it returned output, so a complete post-repair runtime-suite result is not available from this executor.
- 2026-08-01: `graphify update .` could not run because the `graphify` executable is not installed on `PATH`.
- 2026-08-01: Parent integration reran the complete CLI library suite outside sandbox restrictions with `DUCKDB_DOWNLOAD_LIB=1`. All 300 tests passed with no failures or ignores, including all 265 moved white-box tests and the loopback, DuckDB, and temporary-PostgreSQL paths previously blocked by the sandbox.
- 2026-08-01: The aggregate conformance run exposed a cross-crate layout consumer missed by the original split: the P2 data-onramp registry still named 32 CLI test occurrences through the deleted `crates/cdf-cli/src/tests.rs` monolith. Authorized closure repair mechanically changed only those path prefixes to the unique focused source files; all 22 referenced function names, scenario/friction membership, occurrence order, and coverage semantics are unchanged.
- 2026-08-01: Strengthened the existing P2 registry guard from a substring-presence check to require exactly one named definition in every referenced source, and for focused CLI references to require unique resolution across `crates/cdf-cli/src/tests/*.rs`. The first environment-correct exact run reached the independent `cdf-project` registry references and failed on their stale monolith path; CLI-only integrity inspection already reports all 32 occurrences resolved with no stale monolith references or resolution errors. Final shared-guard evidence awaits the independently owned project-layout registry repair.
- 2026-08-01: After the independent project-layout registry repair landed, the exact shared P2 registry guard passed: 1 passed, 0 failed, 0 ignored. The repaired conformance target also passes all-target compilation and strict no-dependency Clippy. The source remains intentionally unformatted at handoff because root owns one combined rustfmt pass after reconciling both path-repair streams; the pre-format check reports only one rustfmt line-wrap in the new integrity helper.
- 2026-08-01: Applied the combined conformance rustfmt pass after both registry repairs were reconciled. The package formatting check and scoped diff-whitespace check now pass, closing the temporary pre-format integration limit without semantic edits.
- 2026-08-01: Authorized repair addressed the fresh review's lexical false-positive finding. Added `syn` as a conformance dev-dependency with only the `full` feature and replaced raw substring counting with syntax-tree inspection: only real function items with the exact identifier and an actual `#[test]` attribute count. Omitted module qualifiers search inline modules recursively; explicit qualifiers navigate only that exact inline module path. Focused CLI uniqueness uses the same recursive structural authority for every candidate file.
- 2026-08-01: The first `--locked` focused run correctly stopped before compilation because Cargo needed to add the already-resolved `syn` package to the conformance lockfile dependency list. An offline run made that metadata-only lock update and exposed a raw-string delimiter error in the synthetic fixture, which was corrected without changing the test cases.
- 2026-08-01: Structural validation exposed one pre-existing non-focused registry qualifier hidden by the old substring check: friction 12 named `scan_command.rs::tests::plan_error_wording_uses_plan_command_name`, while the actual inline owner is `render_tests`. With authorization, corrected only that qualifier; the function name, row membership, and order are unchanged. The structural regression and exact shared registry guard now pass, followed by all-target check, strict Clippy, formatting, and diff hygiene.

## Blockers

None.

## Evidence

- Layout: the 17,654-line `src/tests.rs` is replaced by `src/tests/mod.rs` plus 22 role-named modules. The largest focused test file is `schema_promotion.rs` at 1,632 lines; reusable white-box fixtures remain parent-owned rather than duplicated into command modules.
- Test identity: compiler-independent before/after inventories contain exactly 265 `#[test]` function names with zero additions or removals and zero ignored tests. `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib --locked -- --list` passed and listed 300 total crate tests, including every split white-box test under its focused module.
- Fixture and assertion preservation: sorted top-level `fn`/`struct`/`enum`/`const`/`static` name multisets from the original file and replacement tree compare with no differences. Assertion-site counts are identical before and after: 765 `assert!`, 1,765 `assert_eq!`, 42 `assert_ne!`, 7 `matches!`, and 10 `panic!` invocations.
- Compilation: `cargo check -p cdf-cli --all-targets --locked --message-format short` passed. `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib --locked --no-run` also passed and produced the CLI unit-test executable.
- Strict lint: `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings` passed.
- Layout guards: exact runs of `tests::surface::renderer_migration_gate_rejects_raw_human_output_bypasses` and `tests::surface::destination_registry_composition_is_confined_to_the_cli_root` both passed after teaching their test-source fixtures about `tests/`.
- Runtime-suite limit: the sandboxed full suite reached 269 passed and 31 failed before the two guard repairs; 29 failures were environment setup denials/cascades. The attempted unsandboxed full rerun was interrupted before producing evidence. This limit does not weaken the all-target compilation, test discovery, strict lint, identity, fixture-name, or assertion-count evidence above.
- Closure runtime evidence: `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib --locked -j 12` completed outside sandbox restrictions: 300 passed, 0 failed, 0 ignored. This closes the full unit/integration execution criterion and covers every moved white-box test plus the previously denied loopback and local-PostgreSQL fixtures.
- Cross-crate registry repair: a mechanical integrity probe found 32 focused CLI registry occurrences naming 22 unique functions, zero `crates/cdf-cli/src/tests.rs` references, and zero cases where the named file was absent, contained a non-unique definition, or disagreed with the unique defining file across the focused CLI test tree.
- Shared-guard interim result: `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance run_matrix::data_onramp::p2_registry_named_tests_resolve_to_test_functions --lib --locked -j 12 -- --exact` compiled and executed the repaired guard, then failed on the independently owned stale `crates/cdf-project/src/tests.rs::object_store_gzip_ndjson_discovers_pins_and_executes_through_one_transport` entry before completing. This establishes that linkage and guard execution work but is not final pass evidence for the shared registry.
- Shared-guard closure result: after both path repairs landed, the same exact command passed with 1 passed, 0 failed, 0 ignored, and 102 filtered out. The guard now proves every registry path is readable, every named source contains exactly one matching function definition, and every focused CLI registry function resolves uniquely across the CLI test tree.
- Conformance compile and lint: `CARGO_BUILD_JOBS=12 cargo check -p cdf-conformance --all-targets --locked -j 12` and `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-conformance --all-targets --locked --no-deps -j 12 -- -D warnings` both passed after the CLI repair.
- Formatting closure: `cargo fmt -p cdf-conformance` applied the one required line wrap; `cargo fmt -p cdf-conformance -- --check` and scoped `git diff --check` then passed with no output. The formatting-only pass did not alter registry paths, function names, membership, order, or guard semantics.
- Structural parser regression: `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance run_matrix::data_onramp::p2_registry_test_parser_rejects_decoys_non_tests_and_duplicates --lib --locked -j 12 -- --exact` passed with 1 passed, 0 failed, 0 ignored, and 103 filtered out. Its synthetic Rust proves comment/string decoys and a same-named non-test count as zero, one nested real `#[test]` counts as one both recursively and through its exact module qualifier, and duplicate real tests count as two rather than satisfying exactly-one uniqueness.
- Syntax-aware registry closure: after correcting the stale `render_tests` qualifier, `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance run_matrix::data_onramp::p2_registry_named_tests_resolve_to_test_functions --lib --locked -j 12 -- --exact` passed with 1 passed, 0 failed, 0 ignored, and 103 filtered out. All 32 focused CLI path mappings remain unchanged; their functions are now resolved using parsed Rust test items rather than lexical text.
- Structural repair dependency boundary: `cdf-conformance` adds only `syn = { version = "2.0.117", features = ["full"] }` under dev-dependencies and resolves to the existing locked `syn 2.0.119`; it adds no direct `proc-macro2` dependency.
- Structural repair quality gates: `CARGO_BUILD_JOBS=12 cargo check -p cdf-conformance --all-targets --locked -j 12`, `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-conformance --all-targets --locked --no-deps -j 12 -- -D warnings`, `cargo fmt -p cdf-conformance -- --check`, and scoped `git diff --check` all passed.
- Hygiene: `cargo fmt -p cdf-cli -- --check`, scoped `git diff --check`, the 265-test count assertion, and the zero-ignore assertion all passed.
- Tooling limit: `graphify update .` exited 127 with `command not found`, so graph output was not refreshed by this executor.

## Review

Independent red-team review performed 2026-08-01 using the delegated OCR product-composition/operator-contract rules and a source-level before/after audit.

### Findings

- **Significant — the required post-repair runtime-suite pass is not evidenced.** The acceptance criteria require the CLI unit/integration tests to pass, but the only full runtime execution recorded here predates the two guard repairs and reports 31 failures. The executor credibly classified 29 as sandbox-denied loopback/Postgres setup or the resulting poisoned-mutex cascade, and the two repaired guards pass exactly, but the unsandboxed full rerun returned no result. Compilation, test discovery, strict lint, exact guard runs, and structural preservation do not prove that the remaining runtime tests pass. `Blockers: None` therefore overstates closure readiness; keep this ticket active until a complete post-repair run can execute in an environment that permits its loopback and temporary-Postgres fixtures, or until governing acceptance explicitly permits that verification limit.
- No critical or significant source-preservation defect was found. The review's token-aware inventory found all 421 original top-level function names in the replacement tree. Attributes and bodies are token-identical for 419 functions; the only changed bodies are `renderer_migration_gate_rejects_raw_human_output_bypasses` and `destination_registry_composition_is_confined_to_the_cli_root`, whose changes only exclude the replacement `tests/` source subtree. Their pattern inventories, production exceptions, and assertions are unchanged.
- Fixture parity is stronger than the recorded name/count evidence: all 27 top-level non-function items (`static`, `macro_rules!`, `const`, `struct`, `enum`, `type`, and `impl`) have identical normalized token bodies before and after. The 265 `#[test]` functions remain present, and no `#[ignore]` or `#[should_panic]` attribute was introduced or removed.
- The module map preserves white-box authority and has no focused catch-all. `lib.rs` still owns a private `#[cfg(test)] mod tests;`, every one of the 22 private child modules imports the parent with `use super::*;`, and `tests/mod.rs` contains shared fixtures/helpers but no test functions. The child filenames and contained test names consistently match command families or bounded cross-cutting concerns; the largest focused module remains bounded to schema promotion rather than absorbing unrelated tests.
- The environmental classification is consistent with the unchanged fixtures: HTTP fixtures and `free_port` call `TcpListener::bind("127.0.0.1:0").unwrap()`, while `LocalPostgres::start` holds `LOCAL_POSTGRES_START.lock().unwrap()` across loopback/Postgres startup. A sandbox bind/start denial can therefore panic at fixture setup and poison that mutex for later Postgres tests. This supports the classification as an environment/setup cascade, but not a successful runtime-suite result.

### Verdict

**Concerns.** The layout, naming, white-box boundary, guards, test identities, attributes, fixtures, and assertions survive the split without an identified semantic regression. Closure is blocked only by the significant runtime-verification gap above.

### Residual Risk

- No complete post-repair runtime execution currently covers the loopback and temporary-Postgres paths.
- Token-equivalence proves preservation of moved Rust source, not platform behavior or external-process availability.
- Module focus was reviewed from the complete filename/test-name inventory and parent-helper ownership; future growth could still make the larger schema/adaptor modules worth splitting, but the present tree does not cross the ticket's catch-all boundary.

### Final closure review — 2026-08-01

#### Findings

None. The fresh source review found no preservation or layout defect, and the subsequently recorded complete privileged run closes its sole runtime-verification concern.

#### Reconciliation

- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib --locked -j 12` completed with 300 passed, 0 failed, and 0 ignored. This is the required post-repair run, superseding the earlier sandbox-denied fixture failures and the interrupted rerun.
- The passing suite includes all 265 moved white-box tests, both adapted production-source guards, DuckDB-linked cases, loopback HTTP fixtures, and temporary-PostgreSQL paths. It therefore exercises the exact areas left uncovered by the earlier environment.
- The prior structural evidence remains consistent with the runtime result: all test names and non-test fixture names are preserved, 419 unchanged functions are token-identical, the only two changed guard bodies narrow their scan away from the replacement test tree without changing assertions, no ignore or should-panic attribute drift occurred, and all 22 modules retain focused command/contract ownership.

#### Verdict

**Pass.** The split preserves the CLI white-box test inventory, fixtures, assertions, attributes, and production guards; the focused module tree contains no catch-all; strict lint and formatting pass; and the complete 300-test library suite now passes. All ticket acceptance criteria are supported. Status remains active for parent closure judgment.

#### Residual Risk

- The complete run proves the current host's loopback, DuckDB, and temporary-PostgreSQL behavior; it is not a cross-platform matrix.
- Token-equivalence and the passing suite materially support preservation, but they cannot prove behavior outside the existing assertions.
- Future growth could justify another split of the larger schema/adaptor modules; the current module inventory remains coherent and below the reviewed catch-all boundary.

## Retrospective

- Keeping all shared fixtures in the parent test module preserved the original white-box authority and let focused children import it uniformly with `use super::*;`; this avoided duplicating stateful Postgres, HTTP, package, schema, and recovery setup across command families.
- Source-scanning architecture tests must classify test trees, not a single historical test filename. The split surfaced that path-coupling immediately; adapting their input selection retained the same production invariant while making the tests resilient to focused test organization.
- Structural preservation is stronger than a raw test count alone. Function-name, helper/type-name, ignored-state, assertion-site, compiler, and test-list inventories together make accidental omission or assertion loss substantially harder to hide.
- Cross-crate source registries are layout consumers even when they contain no imports. A module split is not complete until path-bearing test inventories are searched and guarded for path existence plus unique function ownership.
- Syntax-aware registry checks must inspect actual test items, not source spelling. Parsing also turned previously ignored module qualifiers into executable authority and surfaced the stale `tests`/`render_tests` mismatch immediately; exact namespaces prevent coverage records from drifting even when the leaf function survives elsewhere in the file.

## CLI Registry-path Closure Review — 2026-08-01

### Findings

- **Significant — `crates/cdf-conformance/src/run_matrix/data_onramp.rs:888-944` does not prove that a named test function exists.** The strengthened guard counts raw occurrences of `fn {function}(` with `str::matches` and locates files with `str::contains`. If the real function is removed and its named file contains exactly one comment or string such as `// fn target(`, both assertions pass: the named file has one occurrence and is the only focused file returned. The guard likewise does not require the surviving function to retain `#[test]`. Duplicate exact occurrences in one file and occurrences across multiple files are rejected, but substring/comment false positives can still falsely certify a missing or de-registered test. Parse Rust syntax and count real function items with the exact identifier and test attribute across the focused CLI files, then add a negative fixture for comment/string decoys and a duplicate real definition.

No mapping defect was found. All 32 CLI registry occurrences preserve the original 22 function names, order, and scenario/friction membership; every function currently has one actual definition in the named focused file; and no `crates/cdf-cli/src/tests.rs::` reference remains. Project registry mappings were intentionally excluded from this verdict.

### Verdict

**Fail.** The mechanical path mapping is correct, but the closure repair's new uniqueness guarantee can pass without an actual registered test definition. The ticket must remain active until the guard is syntax-aware and its false-positive cases are executable regressions.

### Residual Risk

- This review did not rerun the executor's tests, compilation, lint, or formatting commands. The mapping verdict rests on direct source inventory and before/current registry comparison.
- The current 32 CLI mappings are correct despite the guard defect; the risk is that the guard can silently accept a future missing or de-registered registry target.

## CLI Registry Structural Repair Re-review — 2026-08-01

### Findings

- None. `syn::parse_file` now supplies the sole definition authority: comments and strings produce no items, same-named functions without an exact `#[test]` attribute do not count, and duplicate real tests produce a count greater than one. Unqualified references recurse through inline modules, while explicit qualifiers traverse only the named inline-module path and require the function directly in that owner. Focused CLI unique-file resolution calls the same structural counter for every candidate file, so it cannot disagree with the named-file check.
- All 32 focused CLI occurrences retain the same 22 function names, occurrence order, and scenario/friction membership, and no CLI monolith path remains. The only separately authorized registry-identity correction is `scan_command.rs::tests::plan_error_wording_uses_plan_command_name` to the actual `scan_command.rs::render_tests::...` owner; the leaf function and friction-row position are unchanged.
- `syn` is added only under `cdf-conformance` dev-dependencies with the single required `full` feature. Cargo.lock adds only the conformance dependency edge to the already resolved `syn 2.0.119`; no new package tuple or production dependency is introduced.

### Verdict

**Pass.** The significant lexical false-positive finding is closed. Structural parsing now proves actual registered test ownership, the focused CLI mapping remains exact and unique, and the one corrected explicit qualifier reflects existing source authority without changing coverage membership. Status remains active for orchestrator closure.

### Residual Risk

- This re-review did not repeat the executor's focused regressions, shared registry guard, compile, lint, or formatting commands; it relies on direct source/dependency inspection and the ticket's recorded results.
- The structural helper intentionally recognizes explicit Rust `#[test]` function items. Macro-generated or alternate test attributes are outside this registry's current source-authority model; none of the 32 CLI targets uses those forms.
