Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/2026-07-07-general-run-orchestrator.md
Verdict: concerns

# General Run Orchestrator Partial Review

## Target

Review of the partial general run orchestrator implementation in `crates/cdf-project/src/runtime.rs` and `crates/cdf-project/src/runtime_tests.rs`, plus the ticket split for remaining blocked scope.

## Assumptions Tested

- Existing local-file-to-DuckDB behavior must remain available through compatibility wrappers.
- Checkpoint advancement must remain receipt-gated through `CheckpointStore::commit`.
- The run ledger is observational and must not become an alternate state advancement path.
- Destination commits must use package-aware planning before entering the kernel commit-session API.
- A partial implementation must not close the broad general-orchestrator ticket if Postgres or non-file resource streams are still unsupported.

## Findings

- Significant: `.10x/tickets/2026-07-07-general-run-orchestrator.md` remains incomplete. The implementation supports deterministic local file resources into DuckDB and filesystem Parquet, but not Postgres destinations, REST resource streams, or table-backed SQL resource streams. This is now explicitly owned by `.10x/tickets/2026-07-07-general-run-postgres-destination.md` and `.10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md`; the parent ticket is correctly marked blocked, not done.
- Minor: The public legacy `PreparedReceiptSource::DuckDbCommit` remains DuckDB-specific, while project-level reporting uses `ProjectReceiptSource`. This preserves semver and avoids mislabeling Parquet project reports, but it leaves two receipt-source types for future callers to distinguish.
- Minor: `run_project` still validates `CompiledResource` as local-file-only. The fail-closed behavior is correct for this slice, but future REST/SQL work must avoid routing through `CompiledResource::open` defaults and must ratify non-file `SourcePosition` checkpoint semantics before implementation.

## Verdict

Concerns. The implemented slice is internally coherent and strongly verified, but it is a partial result against the broad general-orchestrator ticket. Do not close the ticket until the blocked Postgres and non-file stream children are resolved or the parent scope is explicitly superseded.

## Residual Risk

Postgres `PostgresLoadPlanInput` construction semantics and REST/SQL source-position semantics are still the highest-risk gaps for this ticket. The current evidence does not cover those surfaces.
