Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md

# Make project, Python, and runtime module boundaries explicit

## Scope

Replace production wildcard and internal-prelude imports in `cdf-project`, `cdf-python`, and `cdf-runtime` with explicit imports and explicit public facade exports. Move genuinely shared value/authority imports to narrow owners rather than preserving crate-wide hidden preludes.

## Non-goals

- No project-format, runtime-contract, Python behavior, FFI, secret, or orchestration semantic change.
- No removal of ratified destination-specific project policy fields.
- No universal abstraction that erases runtime or Python boundary semantics.

## Acceptance Criteria

- Scoped production code contains no `use crate::*`, `use super::*`, or `use ...::prelude::*` ownership shortcuts.
- `cdf-project`, `cdf-python`, and `cdf-runtime` crate facades explicitly enumerate intended exports and preserve their public API.
- Internal import direction is acyclic and each shared type has one visible owner.
- Project, runtime, Python, subprocess integration, and strict lint checks pass without weakened tests.

## References

- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/project-cli-observability-security.md`

## Assumptions

- Record-backed: destination-specific project configuration is ratified product syntax, not a concrete adapter dependency.
- User-ratified: public API and behavior must remain stable while ownership edges become explicit.

## Journal

- 2026-08-01: Audit found 26 top-level production wildcard/prelude imports across these crates and broad wildcard facade exports.
- 2026-08-01: Execution started. Read the ticket and all governing knowledge/specification records; scoped work is import/export ownership only, with product/runtime/Python behavior and public names held invariant.
- 2026-08-01: Replaced project and Python crate-root glob imports with direct standard-library, dependency, internal-leaf, and sibling-owner imports. Test-only aggregate imports remain explicitly test-gated.
- 2026-08-01: Replaced the project runtime prelude and the runtime crate prelude in production with module-owned imports. Both legacy preludes are now `cfg(test)` white-box conveniences with documented unused-import allowances.
- 2026-08-01: Replaced every scoped public wildcard export, including `cdf_runtime::foreign`, with an explicit symbol list. Updated internal consumers that had accidentally depended on `pub(crate)` names flowing through those wildcard facades.
- 2026-08-01: Generated rustdoc before and after the facade rewrite and compared normalized root item sets. Public names are identical for `cdf-runtime` (354), `cdf-runtime::foreign` (31), `cdf-python` (41), and `cdf-project` (255).
- 2026-08-01: Focused compilation, unit tests, strict Clippy, formatting, and whitespace checks completed. `cdf-project` test code compiled through the final link, but local linking cannot run that binary because this environment has no discoverable `libduckdb`; all-target check and strict all-target Clippy cover the test target without linking.
- 2026-08-01: Attempted the required `graphify update .`; the repository environment has no `graphify` executable (`command not found`), so graph regeneration is deferred to parent integration or an environment with the CLI installed.
- 2026-08-01: Closure repair replaced the three qualified production `types::*` imports with explicit names. Shared values now have narrow leaf owners: project receipt-source identity, Python bridge boundary values, and runtime staging/capability identities. Model parsing, secret URI splitting, and lockfile diff helpers moved to the modules whose values they serve; capability-dependent bulk preparation methods moved to the capability owner. Public root reexports and behavior remain unchanged.
- 2026-08-01: Added a `syn`-backed architecture regression check over `cdf-project`, `cdf-python`, and `cdf-runtime`. It excludes structurally test-only files/items, rejects every production glob import, resolves direct module edges including public root-facade aliases, and rejects cycles. A synthetic negative test proves the checker detects both a qualified glob and a two-node cycle.
- 2026-08-01: Closure-repair checks passed: the two architecture tests, scoped all-target strict Clippy, `cdf-project` all-target compilation, runtime and Python unit suites, scoped formatting, and whitespace validation. A broader in-sandbox `cdf-project` test run reached 263 passing tests; one local PostgreSQL socket operation was denied and poisoned the shared test lock for six follow-on PostgreSQL tests. That environmental aggregate result is recorded as a limit, not closure evidence. The ticket remains active for independent fresh review.

## Blockers

None.

## Evidence

- Explicit boundary scan: `rg -n 'prelude::\*|use (crate|super)::\*|pub use [^;]+::\*' crates/cdf-project/src crates/cdf-python/src crates/cdf-runtime/src --glob '*.rs'`. No public wildcard export remains. Every remaining import match is inside a `cfg(test)` module, a dedicated test file, or the test-gated Python import block; production imports contain no wildcard/prelude ownership shortcut. This proves the scoped textual boundary, not semantic behavior.
- Public API parity: generated each crate's rustdoc before and after the rewrite, normalized the root `struct|enum|trait|type|constant|fn` links, and compared sets. `cdf-runtime`: 354 before/354 after, no additions or removals; `cdf-runtime::foreign`: 31/31, no delta; `cdf-python`: 41/41, no delta; `cdf-project`: 255/255, no delta. This proves public item-name parity; focused compilation and tests cover type resolution and behavior.
- `cargo check -p cdf-runtime --all-targets --locked` — passed with no warnings.
- `cargo check -p cdf-python --all-targets --locked` — passed; the final clean run had no production warnings.
- `cargo check -p cdf-project --all-targets --locked` — passed across production and test targets.
- `cargo check -p cdf-subprocess --all-targets --locked` — passed.
- `cargo clippy -p cdf-runtime -p cdf-python --all-targets --locked --no-deps -- -D warnings` — passed.
- `cargo clippy -p cdf-project --all-targets --locked --no-deps -- -D warnings` — passed.
- `cargo test -p cdf-runtime --lib --locked` — 151 passed, 0 failed, 2 ignored.
- `cargo test -p cdf-python --lib --locked` — 34 passed, 0 failed, 7 ignored.
- `cargo test -p cdf-project --lib --locked` — Rust compilation completed; final native link failed because `ld64.lld` could not find `-lduckdb`. Limit: this environment cannot execute the project test binary without the external DuckDB library. The passing all-target check and strict all-target Clippy prove the complete Rust test target compiles.
- `cargo fmt -p cdf-runtime -p cdf-python -p cdf-project -- --check` and `git diff --check -- crates/cdf-project crates/cdf-python crates/cdf-runtime .10x/tickets/done/2026-08-01-product-runtime-module-boundaries.md` — passed.
- `graphify update .` — retried after the closure repair and remained unavailable: `zsh: command not found: graphify`. Limit: graph artifacts were not regenerated by this executor.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-project product_runtime --lib --locked -j 12 -- --nocapture` — 2 passed: the synthetic negative checker detected a qualified wildcard and direct cycle, and the real production scan found neither shape in the three scoped crates.
- `cargo check -p cdf-project --all-targets --locked --message-format short` — passed after the ownership repair, compiling the project test target and its runtime, Python, and subprocess dependencies.
- `cargo clippy -p cdf-runtime -p cdf-python -p cdf-project --all-targets --locked --no-deps -- -D warnings` — passed after the ownership repair and architecture-test addition.
- `cargo test -p cdf-runtime --lib --locked` — 151 passed, 0 failed, 2 ignored after the ownership repair.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-python --lib --locked -j 12` — 34 passed, 0 failed, 7 ignored after the ownership repair.
- `cargo fmt -p cdf-project -p cdf-python -p cdf-runtime -- --check` and scoped `git diff --check` — passed after the final repair edits.
- Broader diagnostic only: `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib --locked -j 12` ran 270 tests in the sandbox: 263 passed; the first PostgreSQL-backed failure was `Operation not permitted` while opening its local test socket, and six subsequent PostgreSQL tests observed the poisoned shared lock. Limit: this does not establish a code failure or a fully passing project suite, and it is not used as closure evidence.
- Parent closure evidence: the same full project command was rerun outside sandbox restrictions with `DUCKDB_DOWNLOAD_LIB=1`; 270 passed, 0 failed, 0 ignored. This executes the PostgreSQL-backed paths that the sandbox denied and closes the prior broad-project runtime limit.

## Review

### Findings

- **Significant — production wildcard ownership shortcuts remain.** `crates/cdf-project/src/runtime/orchestration.rs:14`, `crates/cdf-project/src/runtime/replay.rs:7`, and `crates/cdf-project/src/runtime/validation.rs:1` still import `types::*` in production. The evidence command at this ticket's line 54 matches `prelude::*`, bare `use crate::*`/`use super::*`, and public globs, but it cannot match a qualified private glob such as `types::*`; its conclusion that production imports contain no wildcard ownership shortcut is therefore false. These imports keep changes to any runtime report/request type implicitly visible to orchestration, replay, and validation, preserving the hidden-prelude coupling the scope intended to remove. Repair requires enumerating the concrete type imports and broadening the structural check to reject every production glob import, while retaining explicit test-only exceptions.
- **Significant — the internal ownership graph is not acyclic.** Direct strongly connected components remain in every scoped crate. In `cdf-project`, `runtime/destinations.rs:1` imports `ProjectReceiptSource` from `types`, while `runtime/types.rs:1-4` imports `ResolvedProjectDestination` from `destinations`; separately, `internal.rs:13-17` imports `lockfile`/`models`/`secrets` values while `lockfile.rs:15-23`, `models.rs:6-8`, and `secrets.rs:11` import helpers back from `internal`. In `cdf-python`, `bridge.rs:18-22` imports from `dlt` and `internal`, `dlt.rs:13-15` imports from `bridge` and `internal`, and `internal.rs:12` imports `PythonBridgeOptions` back from `bridge`. In `cdf-runtime`, `staging.rs:17` imports lease types owned by `staging_lease`, while `staging_lease.rs:15` imports `LoadAttemptId` owned by `staging`; `bulk.rs:5-8` also consumes facade names owned by `capabilities`, while `capabilities.rs:4-8` imports bulk types. Compilation proves Rust can resolve these cycles, not that the acceptance criterion at line 22 is met or that shared values have a lower, single owner. Repair requires extracting the shared values/helpers into narrow leaf owners and importing those owners directly rather than through sibling or crate-root facades.
- **Minor — boundary acceptance has no executable regression assertion.** The only ticket-owned test hunk visible in the scoped diff is an import adjustment at `crates/cdf-runtime/src/tests.rs:2`; no test or checked architecture tool asserts production-glob absence, module-graph acyclicity, or the explicit facade inventory. The under-matching scan above consequently reported success despite three production globs and several direct cycles. Add an AST-aware boundary check (with explicit `cfg(test)` handling), an acyclic module-edge assertion, and a reproducible public-facade snapshot/consumer check so these acceptance criteria cannot be satisfied by an ad hoc textual scan alone.

### Verdict

**Fail.** The diff is import/export-only, and review found no introduced project-format, secret-redaction, runtime dependency-layer, Python behavior, or FFI ownership semantic change. The explicit crate facades enumerate current names, and the recorded rustdoc counts plus successful compilation provide reasonable evidence of current root item-name parity. However, two central ticket outcomes are not achieved: production wildcard coupling remains and the internal dependency graph is demonstrably cyclic. The ticket must remain active.

### Residual Risk

- The public-parity evidence preserves normalized root item names only; the before/after inventories and exact normalization procedure were not retained, so this review could not independently falsify downstream signature/namespace/semver compatibility from the durable record alone. The fact that public definitions were not edited and all scoped crates compiled reduces, but does not eliminate, that risk.
- `cdf-project`'s runtime test binary was not executed because the environment could not link `libduckdb`; its all-target compilation covers Rust type resolution but not runtime assertions.
- `graphify` was unavailable, so the graph artifacts could not independently expose or confirm the source-level ownership cycles listed above.

### Closure-repair re-review — 2026-08-01

Fresh independent review used OCR delegation for deterministic workspace selection and the project, hostile-FFI, and runtime rule sets, then scoped source inspection to the repaired owners and `crates/cdf-project/src/tests/project_files.rs`. This reviewer did not author the repair or its prior review.

#### Findings

None.

#### Falsification evidence

- The three previously missed qualified production globs are gone. `runtime/orchestration.rs`, `runtime/replay.rs`, and `runtime/validation.rs` now enumerate their `types` imports, and an all-source wildcard scan found only structurally test-gated/test-file globs in the three scoped crates. The `syn` checker independently reported no production glob violation.
- Every named project SCC is dissolved. `destinations -> receipt_source` while `types -> destinations + receipt_source`; the receipt value owner has no imports. Separately, `lockfile -> internal -> models/secrets/sources`, while the model parsers, secret URI splitters, and lockfile diff helpers now live privately with their values and no repaired leaf imports back into `internal`.
- The Python graph is now `bridge -> dlt/internal/bridge_types`, `dlt -> internal/bridge_types`, and `internal -> bridge_types`. `bridge_types` owns the shared bridge values and identifier sanitizer without importing a Python sibling. The Arrow capsule unsafe owner is untouched, and the bridge implementation after the moved value definitions is unchanged.
- The runtime graph is now `capabilities -> bulk/capability_types/staging/execution_host`, `bulk -> capability_types/execution_host`, `staging -> staging_identity/staging_lease`, and `staging_lease -> staging_identity/execution_host`. `capability_types` and `staging_identity` are dependency leaves. The capability-dependent `BulkPathPreparation` methods moved intact to the capability owner; validation and selection order are unchanged.
- The architecture regression genuinely recurses. `collect_rust_files` recursively descends the three source trees; `syn::visit::Visit` recursively visits every non-test inline module/function and `flatten_use_tree` recursively descends paths and groups to their leaves, so `crate::types::*` and any other qualified glob are detected rather than only bare root globs. Test-only paths and `cfg` items are excluded structurally, not by accepting a production wildcard allowlist.
- Cycle detection is not limited to the synthetic two-node example. The DFS walks every graph node and keeps the complete active path; any back-edge returns the whole cycle slice, including cycles longer than two modules. The real-source assertion passed with no project, Python, or runtime cycle. A separate inspection of fully qualified `crate::...` and `super::...` references outside imports found no hidden reverse edge among the repaired ownership families.
- Public/API semantics remain stable at the reviewed boundary. `ProjectReceiptSource`, all Python bridge values/constants, `DestinationIngressMode`, `DestinationWriterModel`, and `LoadAttemptId` retain their prior derives, fields/variants, validation, and serialization attributes and remain explicitly re-exported at the same crate roots. The moved project model, secret, and lockfile helpers retain their prior bodies while narrowing to their actual private owners. No FFI callback/unsafe code, secret message/URI rule, runtime scheduling, staging lease, or bulk selection algorithm changed in the repair.
- The focused architecture command recorded by the executor remains valid acceptance evidence. During this review it also selected both named `product_runtime` tests and passed 2/2; the verdict rests on source/assertion inspection rather than treating that repeated run as independent semantic proof.

#### Verdict

**Pass.** The significant wildcard and cyclic-ownership findings are repaired. The new leaf owners are accurate, the direct production import graph is acyclic across all three crates, the `syn` regression would reject both qualified production globs and cycles of arbitrary length, and no public API, FFI, secret, or runtime semantic regression was found. The ticket remains active for parent closure judgment.

#### Residual Risk

- The architecture graph deliberately models source-level `use` edges and direct root-facade aliases; macro-expanded imports and fully qualified non-`use` paths are not graph edges. No scoped macro generates imports, and independent inspection found no current hidden reverse edge, but future code using those forms would need the checker extended to preserve complete dependency coverage.
- Public parity still depends partly on the executor's normalized rustdoc inventories, which are summarized but not stored. Direct comparison found the repair's moved public definitions and explicit root reexports unchanged, materially narrowing the remaining signature/namespace risk.
- The pre-existing environment limits remain: the broad project suite's native/PostgreSQL execution is not clean closure evidence, and `graphify` is unavailable. Neither limit exposed a repair-specific defect in this source review.

### Final closure review — 2026-08-01

#### Findings

None. The source repair had already passed fresh independent review, and the subsequently recorded full project execution closes its only remaining runtime-evidence limit.

#### Reconciliation

- The complete project library command ran with DuckDB linkage available and outside the sandbox restriction that had denied the local PostgreSQL socket. It completed 270 tests with 270 passed, 0 failed, and 0 ignored. This supersedes both earlier environmental observations: the native DuckDB link failure and the in-sandbox PostgreSQL denial/poisoned-lock cascade.
- The prior source verdict remains supported: production wildcard/prelude imports are absent, explicit facades preserve the reviewed public definitions, the repaired project/Python/runtime ownership graph is acyclic, and the recursive `syn` regression rejects both qualified production globs and cycles.
- Existing focused evidence remains green for runtime, Python, project all-target compilation, subprocess integration compilation, strict lint, formatting, and patch hygiene. No test or lint was weakened to obtain the complete project result.

#### Verdict

**Pass.** The repaired source boundaries passed fresh review, and the full 270-test project suite now passes with the previously unavailable DuckDB and PostgreSQL paths exercised. All ticket acceptance criteria are supported. Status remains active for parent closure judgment.

#### Residual Risk

- The architecture regression models source-level imports and direct root-facade aliases rather than arbitrary macro expansion or every fully qualified expression path. Direct inspection found no hidden reverse edge in the repaired families.
- Exact external semver compatibility remains supported by public-definition comparison, normalized rustdoc item inventories, and downstream compilation rather than a separately retained semver-diff artifact.
- The runtime and Python unit suites retain their pre-existing ignored tests; the complete project suite itself had no ignored tests. This closure establishes ordinary boundary behavior, not the performance/stress behavior represented by ignored cases.

## Retrospective

The public facade rewrite was larger than the production import cleanup, but treating rustdoc as a before/after API inventory made it deterministic. The important surprise was that source-file mapping alone omits upstream reexports and `pub(crate)` names: compiler diagnostics exposed internal consumers that had been using the crate root as an accidental private prelude, while exact rustdoc set comparison exposed public aliases originating in `cdf-kernel`, `cdf-runtime`, and `cdf-foreign-stream`. Keeping those two checks separate prevented both hidden internal coupling and accidental API loss.

Compile-guided replacement was effective once dependent crates were stable. Parallel destination refactoring temporarily made project diagnostics noisy; using `--lib` isolated production project verification, then `--all-targets` closed the test-compile gate after dependencies stabilized. The one remaining execution limit is environmental native linking, not Rust code or test-target compilation.

Parent integration subsequently removed that execution limit: with native linkage available and PostgreSQL socket access permitted, the complete project suite passed 270/270. The earlier limitation remains useful history but is no longer a closure constraint.
