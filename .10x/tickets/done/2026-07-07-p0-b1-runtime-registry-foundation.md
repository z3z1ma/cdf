Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md
Depends-On: .10x/decisions/superseded/project-destination-driver-registry.md

# P0 B1: Runtime registry foundation and module split

## Scope

Create the `cdf-project` runtime foundation required by Workstream B without changing run/replay/recover behavior.

Owns:

- splitting `crates/cdf-project/src/runtime.rs` into focused runtime modules;
- adding the project-level destination driver/runtime adapter traits ratified historically in `.10x/decisions/superseded/project-destination-driver-registry.md`;
- adding shared runtime stage/failpoint hook types;
- preserving the current public API temporarily while downstream children migrate callers.

## Acceptance criteria

- `runtime.rs` becomes a module facade or is replaced by `runtime/mod.rs`; no single runtime module owns orchestration, replay, recovery, failpoints, state-delta construction, ledger recording, and destination adapters at once.
- Runtime submodules follow `.10x/knowledge/rust-crate-organization.md` and have focused responsibilities: types, resource abstraction/resolution, destination registry/adapters, run orchestration, replay/recovery, ledger, state delta/artifacts, receipts, and destination-specific adapters.
- `ProjectDestinationDriver`, `ProjectDestinationRuntime`, `PreparedDestinationCommit`, destination description/reporting policy, and generic runtime stage hook types exist in `cdf-project`.
- The existing public run/replay/recover APIs still compile after the split so B2-B4 can migrate callers deliberately.
- `cargo check -p cdf-project --all-targets` passes.

## Evidence expectations

Record before/after module shape, public API inventory after the split, `cargo check -p cdf-project --all-targets`, `git diff --check`, and focused complexity output for `cdf-project/src/runtime*`.

## Explicit exclusions

No caller migration, no deletion of specialized public wrappers, no new destination, no behavior changes, and no CLI command changes.

## Progress and notes

- 2026-07-07: Opened from Workstream B after read-only inventories by Huygens, Newton, and Euler. Newton identified module split candidates and the `runtime.rs` hotspot; Euler recommended the project-level destination driver adapter shape.
- 2026-07-07: Worker implemented B1 runtime foundation without caller migration or behavior changes. `crates/cdf-project/src/runtime.rs` is now a facade over focused modules: `artifacts.rs`, `destinations.rs`, `hooks.rs`, `ledger.rs`, `orchestration.rs`, `receipts.rs`, `replay.rs`, `resources.rs`, `types.rs`, and `validation.rs`.
- 2026-07-07: Added project destination foundation API in `runtime/destinations.rs`: `ProjectDestinationDriver`, `ProjectDestinationRuntime`, `PreparedDestinationCommit`, `ProjectDestinationDescription`, `DestinationReceiptReportingPolicy`, `ProjectResolutionContext`, and `DestinationPlanningContext`. Added generic runtime hook API in `runtime/hooks.rs`: `RuntimeStage` and `RuntimeStageHook`. Existing run/replay/recover wrapper exports remain available through the runtime facade.
- 2026-07-07: Focused size output after split: facade 77 lines; artifacts 474; destinations 283; hooks 22; ledger 153; orchestration 443; receipts 148; replay 1076; resources 42; types 255; validation 210; total runtime files 3183 lines.
- 2026-07-07: Public API inventory after split was checked with `rg -n "^pub (type|struct|enum|trait|fn)|^pub use" crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime/*.rs`; legacy public request/report/run/replay/recover symbols remain re-exported and new driver/runtime foundation symbols are public.
- 2026-07-07: Verification passed: `cargo fmt --check`; `cargo check -p cdf-project --all-targets`; `git diff --check`; optional `cargo test -p cdf-project --no-fail-fast` with 62 unit tests and 0 doc-tests passing.
- 2026-07-07: Review found the initial pending-context field was not usable by generic B2 code because `ProjectDestinationRuntime` had no binding method before `protocol().begin`. Fixed the foundation with `PreparedDestinationCommit::{new, with_pending_context, take_pending_context, has_pending_context}` plus required `ProjectDestinationRuntime::bind_prepared_commit(&mut PreparedDestinationCommit)`, so an adapter can consume its pending context before generic code begins the kernel session.
- 2026-07-07: Parent verification and closure evidence recorded in `.10x/evidence/2026-07-07-p0-b1-runtime-registry-foundation.md`; adversarial review and resolution recorded in `.10x/reviews/2026-07-07-p0-b1-runtime-registry-foundation-review.md`.

## Blockers

None.
