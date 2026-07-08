Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-p0-b1-runtime-registry-foundation.md
Verdict: concerns

# P0 B1 Runtime Registry Foundation Review

## Target

Current B1 diff splitting `crates/cdf-project/src/runtime.rs` and adding the project destination registry foundation API.

## Findings

- Significant, closure-process: The initial review saw `runtime.rs` declaring split modules while the new `crates/cdf-project/src/runtime/*.rs` files and `.10x/evidence/2026-07-07-p0-b1-runtime-registry-foundation.md` were untracked. This blocks closure until the split files and evidence are staged/committed with the patch.
- Significant, foundation API: The initial `PreparedDestinationCommit` carried `pending_context`, but `ProjectDestinationRuntime` exposed only `protocol()` and `prepare_package_commit(...)`. That did not give B2 a generic way to bind adapter-owned context before `DestinationProtocol::begin(commit, plan)`, which the ratified decision requires for Postgres-like package-aware planning.
- Minor, scope hygiene: `.gitignore` and `cdf-repo-selection-2026-07-07.zip` are unrelated to B1 and must remain unstaged/uncommitted.

## Resolution

The API concern was addressed in `crates/cdf-project/src/runtime/destinations.rs`:

- `PreparedDestinationCommit` now exposes `new`, `with_pending_context`, `take_pending_context`, and `has_pending_context`.
- `ProjectDestinationRuntime` now requires `bind_prepared_commit(&mut PreparedDestinationCommit)`.

This gives B2 a concrete generic flow: prepare the destination commit, let the adapter bind and consume its pending context, then call `runtime.protocol().begin(prepared.commit, prepared.plan)`.

The process concern is resolved by staging and committing the split runtime files and evidence record with B1. The unrelated `.gitignore` edit and repo-selection zip remain outside the B1 staged scope.

## Verdict

Concerns raised and resolved for B1 closure. Remaining specialized replay/recovery wrappers, closed run resource/destination enums, CLI/conformance migration, and generic replay skeleton wiring are downstream Workstream B child scopes, not B1 blockers.

## Residual risk

B1 only provides the foundation API and module split. B2 must prove the pending-context binding flow with real DuckDB, Parquet, Postgres, and mock destination adapters. B4 must delete the compatibility wrappers after caller migration.
