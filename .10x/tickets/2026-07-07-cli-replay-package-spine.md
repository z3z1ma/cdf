Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md
Depends-On: .10x/tickets/done/2026-07-07-general-run-orchestrator.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Wire `cdf replay package` to package replay

## Scope

Replace the `replay package` not-supported stub with CLI routing to package artifact replay functions for supported destinations.

Owns:

- `crates/cdf-cli/src/args.rs`
- `crates/cdf-cli/src/commands.rs`
- `crates/cdf-cli/src/context.rs` if destination URI parsing is shared with `run`.
- Focused CLI tests for replay success, duplicate/no-op reporting, and fail-closed inputs.

## Acceptance criteria

- `cdf replay package <pkg> --to <dest>` is parsed and rejects missing `--to`.
- DuckDB and filesystem Parquet package replay create/advance checkpoint state through the replay APIs without contacting source resources.
- Postgres replay requires explicit CLI inputs for the Postgres policy that package artifacts do not own, including target and merge dedup policy, and fails closed before mutation when those inputs are absent or inconsistent.
- Duplicate package-token receipts are represented in JSON/human output when the destination exposes duplicate/no-op status.
- Replay JSON includes package hash, destination id, target, receipt id, checkpoint id/status, receipt source, duplicate status when known, and package status.

## Evidence expectations

Run focused replay CLI tests for DuckDB, Parquet, Postgres fail-closed policy, and duplicate replay where deterministic; run relevant `cdf-project` artifact replay tests, clippy for CLI/project, workspace check, and `git diff --check`.

## Explicit exclusions

No `resume`, no `inspect run`, no new package artifact schema, no source extraction, no destination introspection as a source of missing Postgres semantics.

## Design notes

- Current parser accepts only `replay package <DIR>` and the command returns not-supported.
- `cdf_project` now exposes DuckDB, Parquet, and Postgres artifact replay functions. Postgres replay intentionally requires explicit policy input by `.10x/decisions/project-run-postgres-destination-inputs.md`.
- The command currently does not receive `Cli`; implementation likely needs dispatch to call `replay_package(&cli, args)` so it can use selected project environment state and secret providers.

## Blockers

Postgres CLI flag shape for explicit target/dedup policy must be settled in this ticket before enabling Postgres replay. DuckDB replay can proceed without that decision.

## Progress and notes

- 2026-07-07: Split from the broad CLI spine ticket after package-artifact replay became available for all current project-run destinations.
