Status: done
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/done/2026-07-26-pre-wave-architecture-hardening-program.md`

# Make driver concurrency law executable

## Scope

Document the source/destination concurrency canon in authoring surfaces and add compile-time and
runtime conformance that proves driver, runtime, finalized-session, staged-session, execution-host,
and portable-plan responsibilities.

## Non-goals

- No new executor, concurrency knob, ingress mode, or blanket `Send + Sync` bound.
- No requirement that a destination implement both ingress categories.
- No destination-identity branch in conformance or orchestration.

## Acceptance criteria

- Public trait documentation states ownership, thread movement, native-handle confinement,
  injected-host, and fail-closed portability laws.
- Compile assertions prove the intended positive bounds without asserting unintended runtime or
  finalized-session bounds.
- Synthetic staged and finalized destinations pass applicable capability-discovered laws or an
  exact sheet-declared exclusion.
- A synthetic source proves synchronous contact-free compile and explicit portability validation
  before isolated execution.
- Existing destination/source conformance remains data-driven and no generic match arm is added.

## References

- `.10x/decisions/driver-session-concurrency-canon.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/destination-extension-runtime-contract.md`
- `.10x/decisions/destination-ingress-protocol-capability-split.md`

## Assumptions

- Source-backed: current trait bounds already implement the desired law.

## Journal

- 2026-07-26: Shaped as documentation/conformance of existing behavior, not a trait-bound rewrite.
- 2026-07-26: Activated after A2 closure. Governing records confirm this ticket documents and
  tests the current minimal bounds; it does not add blanket `Send + Sync` constraints or a second
  ingress category. `graphify query "driver session concurrency conformance A3"` could not run
  because the `graphify` executable remains unavailable, so source and Cargo inspection own the
  implementation inventory.
- 2026-07-26: Added public concurrency and ownership law to destination/source drivers, resolved
  runtimes, finalized/staged ingress, commit/staging sessions, and the execution host. The
  documentation distinguishes ingress-protocol confinement from declared blocking-lane
  confinement and never treats host thread safety as native-handle mobility.
- 2026-07-26: Added positive trait-object compile assertions, `Rc`-backed non-`Send` finalized
  runtime/session fixtures, capability-discovered staged/finalized laws, a contact counter around
  synchronous source compilation, and worker-admission portability counters plus a default
  fail-closed driver.
- 2026-07-26: `graphify update .` also failed with `command not found`; no generated graph was
  changed.
- 2026-07-26: Delegated OCR found inaccurate source-contact wording and two weak conformance
  claims, then found an incomplete native-handle confinement sentence on re-review. All findings
  were repaired by the executor. The third review returned no findings and `pass`.

## Blockers

None.

## Evidence

- Public authoring law: `cargo doc -p cdf-kernel -p cdf-runtime --no-deps --locked` generated both
  crates without warnings. Source inspection maps the law to `DestinationDriver`,
  `DestinationRuntime`, both ingress traits, `CommitSession`, `StagedIngressSession`,
  `ExecutionHost`, and `SourceDriver`. This proves rendered public documentation and its Rustdoc
  links, not every external adapter's prose.
- Compile bounds: `cargo test -p cdf-runtime --lib
  tests::driver_concurrency_canon_is_compile_and_capability_enforced --locked -- --exact` passed.
  The test asserts the positive driver/staged-session/host trait-object bounds and compiles
  `MockRuntime` plus `MockFinalizedSession` with `Rc` state through their deliberately unbounded
  traits. This guards the trait wall; it does not prove every concrete adapter's handle
  confinement.
- Capability-discovered ingress: the same focused test passed for synthetic staged and finalized
  runtimes. It validates declared capabilities, selects by `DestinationIngressMode`, checks the
  staged byte/segment ceilings, and checks the finalized serial writer exclusion without a
  destination identity branch.
- Source compile and portability: `cargo test -p cdf-runtime --lib
  tests::source_registry_compiles_hashes_and_resolves_mock_without_order_authority --locked --
  --exact` passed and observed zero contact-boundary calls during compile and portable-plan
  validation. `cargo test -p cdf-runtime --lib
  worker_protocol::tests::isolated_worker_reconstructs_every_authority_from_artifacts --locked --
  --exact` passed and observed the owning driver validation exactly once through worker authority
  reconstruction; a driver retaining the default validator failed before isolated execution.
- Regression gates: `cargo test -p cdf-runtime --lib --locked --quiet` passed 149 tests with two
  intentional ignores; `cargo clippy -p cdf-kernel -p cdf-runtime --all-targets --all-features
  --locked -- -D warnings` passed; `cargo fmt --all -- --check` passed.
- Generic conformance: `cargo check -p cdf-conformance --tests --locked` passed and the existing
  `generic_conformance_engines_do_not_branch_on_destination_identity` assertion remains compiled.
  Attempting to execute that test stopped before test startup because this host's DuckDB test
  linkage has unresolved `duckdb_*` symbols. This limits runtime evidence for that separate
  harness; the focused runtime conformance and non-linking full conformance compile are green.

## Review

- Delegated OCR pass 1 (`concerns`): medium findings for inaccurate pre-resolution source-contact
  documentation, `Send`-capable negative fixtures that did not guard blanket bounds, and a
  portability test that called a registry helper instead of worker admission. Repaired with
  lifecycle-accurate prose, `Rc` thread-affinity fixtures, and worker-path validation/fail-closed
  assertions.
- Delegated OCR pass 2 (`concerns`): medium finding that native-handle docs named only
  blocking-lane confinement while the decision and fixture also permit ingress-protocol
  confinement. Repaired all affected public wording.
- Delegated OCR pass 3: no findings; verdict `pass`. Residual risk: trait-level conformance cannot
  prove each adapter avoids private executors or correctly confines every native handle, so those
  remain adapter and shared conformance-review obligations.

## Retrospective

- What surprised: omission is not a compile wall. The original test avoided asserting
  `DestinationRuntime: Send`, but its fixtures were accidentally `Send`, so a future blanket
  bound would still compile.
- What worked: an `Rc` marker expresses legitimate thread affinity without unsafe code or a
  negative-compilation harness; capability-enum matching exercises both ingress laws without
  adding an identity switch; atomics made lifecycle stage separation observable.
- Dead end: direct `SourceRegistry::validate_portable_source_plan` coverage proved the helper but
  not isolated-worker admission. Moving the counter into the worker-protocol fixture closed the
  actual bypass seam.
- Five whys: the first docs overclaimed source contact because “compile versus runtime” collapsed
  health, discovery, resolution, and execution into two buckets. Naming each lifecycle stage is
  the durable prevention.
- Distillation: updated `.10x/knowledge/source-destination-extension-invariant.md` with executable
  positive/negative trait-wall and worker-admission guidance. No new skill was created: this is
  durable design/review judgment, not a recurring operational procedure with stable commands.
