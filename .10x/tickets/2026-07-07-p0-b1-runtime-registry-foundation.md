Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md
Depends-On: .10x/decisions/project-destination-driver-registry.md

# P0 B1: Runtime registry foundation and module split

## Scope

Create the `cdf-project` runtime foundation required by Workstream B without changing run/replay/recover behavior.

Owns:

- splitting `crates/cdf-project/src/runtime.rs` into focused runtime modules;
- adding the project-level destination driver/runtime adapter traits ratified in `.10x/decisions/project-destination-driver-registry.md`;
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

## Blockers

None.
