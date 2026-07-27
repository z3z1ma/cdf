Status: done
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Enforce Rust safety and panic lint walls

## Scope

Install workspace lint inheritance, explicit unsafe-code exceptions for the three production FFI
owners, and non-test unwrap/expect denial for foundational and extension-contract crates. Repair
the finite production violations needed to activate the walls.

## Non-goals

- No denial of unwrap/expect in test modules.
- No enablement of the complete Clippy pedantic catalog.
- No redesign of measured FFI paths or weakening of their performance.

## Acceptance criteria

- Every crate explicitly inherits the workspace policy or declares a named FFI exception.
- Unsafe production code exists only in narrow modules under `cdf-dest-duckdb`, `cdf-python`, and
  `cdf-subprocess`; benchmark exceptions are measurement-only.
- Every unsafe block has a safety rationale and governing record reference.
- Named foundational crates compile with non-test unwrap/expect denied.
- Architecture tests enumerate the exception set and fail on a new exception.
- Focused tests, formatting, and strict workspace Clippy pass with the same release features used
  by CI.

## References

- `.10x/decisions/compiler-enforced-rust-safety-walls.md`
- `QUALITY.md`

## Assumptions

- Source-backed: the current unsafe owner set is finite and already isolated by module.
- Record-backed: no performance regression is acceptable solely for lint aesthetics.

## Journal

- 2026-07-26: Source inventory found production unsafe only in DuckDB segment scan/envelope,
  Python Arrow capsule, and subprocess runner; workspace lint inheritance is absent.
- 2026-07-26: Activated after A1 closure. The compiler-wall design is record-backed by
  `.10x/decisions/compiler-enforced-rust-safety-walls.md`; implementation will keep the existing
  measured FFI owners and make every exception mechanically enumerable.
- 2026-07-26: Added workspace-owned Rust/Clippy lint policy and explicit inheritance to all 51
  member manifests. Local unsafe allowances are restricted to the three production FFI owners
  and the benchmark reference module, with decision citations and block-level safety rationales.
- 2026-07-26: Activated non-test `unwrap_used`/`expect_used` denial in the eight governed crates.
  Compiler-guided repair converted recoverable production extraction to typed errors or explicit
  lifecycle recovery. The required fallibility propagated through destination staging,
  file/Glue/Iceberg task readers, runtime source frontiers, and the standalone execution host
  rather than being hidden behind lint allowances.
- 2026-07-26: The engine suite exposed one stale scratch-accounting assertion: canonical segment
  output may retain traveling input arrays, so construction scratch is intentionally positive but
  smaller than the complete durable output rather than equal to it. The assertion now enforces
  that existing ownership contract; the complete engine library suite passes.
- 2026-07-26: Hardened the architecture test to enumerate both source files containing unsafe
  operations and source files declaring local `unsafe_code` allowances. This closes the bypass
  where a new narrow allowance could otherwise accompany differently formatted unsafe syntax.
- 2026-07-26: Initial adversarial review found that filename-only unsafe enumeration was still
  bypassable, mechanical poisoned-lock recovery could reuse inconsistent accounting/executor
  state, unsafe functions lacked caller-level contracts, and task authority fallibility exposed an
  impossible public error. Repaired all four: the gate now parses Rust syntax and macro tokens into
  exact allowance/function/block inventories, every unsafe function carries `# Safety`
  preconditions, poisoned fallible locks propagate while infallible invariant locks fail-stop, and
  the task reader constructs a non-optional authority without changing its accessor API.
- 2026-07-26: `graphify update .` could not run because the `graphify` executable is unavailable
  in this environment; this is the same recorded tool limitation as A1, not a graph-source claim.

## Blockers

None.

## Evidence

- Workspace inheritance and exception closure: `cargo test -p cdf-project --features
  cdf-dest-duckdb/bundled-duckdb
  tests::workspace_safety_lint_policy_and_exception_set_are_closed --locked -- --exact` passed
  (1 passed). The test parses every member manifest, asserts the two workspace lint defaults,
  enumerates every exact allowance target, unsafe function and contract, explicit unsafe block,
  unsafe token inside macros, unsafe foreign module, unsafe impl, and unsafe trait; it requires
  governing decision citations and checks the eight production panic-denial crate roots.
- Foundational production panic wall: `cargo clippy -p cdf-kernel -p cdf-memory -p cdf-runtime -p
  cdf-package -p cdf-package-contract -p cdf-engine -p cdf-task-store -p cdf-object-access --lib
  --locked -- -D warnings` passed. This compiles the governed non-test library surfaces with
  unchecked unwrap/expect denied.
- Complete feature/target compiler wall: `cargo clippy --workspace --all-targets --all-features
  --locked -- -D warnings` passed. This compiles the production FFI exceptions, benchmark
  exception, tests, examples, and all feature combinations; all-features also unifies bundled
  DuckDB linkage.
- Governed regression suites: the final combined eight-crate library run passed for kernel (75),
  memory (25), runtime (148; 2 ignored), package (83; 4 ignored), package-contract (10), task-store (7; 1
  ignored), and object-access (39; 1 ignored). After correcting the stale engine ownership
  assertion, `cargo test -p cdf-engine --lib --locked` passed (205 passed, 7 ignored).
- Formatting and manifest integrity: `cargo fmt --all -- --check`, `cargo metadata --locked
  --no-deps --format-version 1`, and `git diff --check` passed.
- Limit: the architecture test without bundled DuckDB reached the link stage but the local host has
  no system `libduckdb`; the recorded command uses the release-equivalent bundled feature and
  passed. No claim is made about a system-DuckDB development installation.
- Limit: `graphify update .` returned `command not found`; source, Cargo metadata, compiler, tests,
  and OCR review provide the recorded evidence, while `graphify-out/` could not be refreshed.

## Review

- Delegated OCR pass 1 verdict: `fail`.
  - Significant: mechanical `PoisonError::into_inner` recovery could reuse partially mutated
    memory/coordinator/executor state.
  - Significant: literal/file-level unsafe enumeration was bypassable within an approved file.
  - Significant: 17 unsafe DuckDB/benchmark functions lacked caller-level safety contracts.
  - Minor: making verified task authority fallible exposed an impossible public error.
- Corrections: fallible poison paths now propagate and invariant paths fail-stop; a poison
  regression test protects lease accounting. The architecture gate uses AST plus recursive macro
  token inventory and exact allowance/function/contract/block counts. Every unsafe function has a
  `# Safety` contract. The task reader uses a construction cursor and stores non-optional authority
  while preserving the original accessor.
- Delegated OCR pass 2 verdict: `fail`.
  - Significant: bare-name sets could collapse duplicate unsafe functions/allowance targets and did
    not independently inventory unsafe method contracts.
- Correction: unsafe identities are sorted multisets, so duplicates change cardinality; unsafe impl
  and trait methods contribute independent allowance/contract identities.
- Delegated OCR final verdict: `pass`, no findings. The reviewer confirmed the collision bypass,
  task-store authority model, and poisoned-lock fail-stop repair are closed.
- Residual risk: reviewer inspection was read-only and relied on the journaled focused tests and
  strict Clippy results. The local system-DuckDB and unavailable-graphify limits remain as recorded
  in Evidence.

## Retrospective

- What broke: converting the posture into a compiler wall exposed fallible extraction well beyond
  the eight crate roots because their public lifecycle values flow into destination and source
  adapters. Treating only the initially named files would have replaced compiler errors with
  allowances and left the actual boundary incomplete.
- What surprised: staging-mutation lease cloning and external task authority access were modeled
  as infallible even though the former depends on checked accounting. A narrow `try_clone` made
  that contract honest. Review then showed task authority is different: verified construction
  makes its presence structural, so a construction cursor preserves a non-optional final field and
  the original infallible accessor without a compatibility shim.
- Dead ends: the first focused architecture-test command linked against a missing system DuckDB;
  the bundled release feature is the reproducible local route. An engine scratch-memory test also
  encoded a superseded equality assumption and was repaired only after inspecting the live
  ownership comment and observed accounting.
- What worked: enabling the compiler wall early produced a finite inventory; repairing from
  foundational crates outward kept propagation legible. The initial filename scan was too weak;
  AST plus macro-token inventory and exact per-function contracts made the gate resistant to
  alternate formatting and unsafe hidden in already-approved files.
- Review lesson: replacing poisoned-lock `unwrap` with `PoisonError::into_inner` is not a lint
  repair; it changes fail-stop behavior into potentially corrupt recovery. Fallible APIs now return
  an internal error, infallible invariant surfaces explicitly refuse recovery, and a regression
  test proves lease reconciliation does not reuse stale accounting after poison.
- Five whys: unchecked extraction persisted because lint policy was per-crate/implicit; that made
  new packages opt out by omission; omission was invisible because no manifest gate existed; the
  source-only unsafe inventory did not enumerate allowances; therefore the durable repair combines
  root policy, explicit inheritance, compiler denial, and a closed-set architecture test.
- Distillation: `.10x/knowledge/rust-safety-lint-walls.md` records the authoring and verification
  rules. No new procedural skill is warranted: the recurring action is a compiler/test gate already
  executable from the knowledge record, not a failure-prone multi-step operational runbook.
