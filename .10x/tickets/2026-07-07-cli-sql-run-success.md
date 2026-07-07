Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md
Depends-On: .10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md, .10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md, .10x/tickets/done/2026-07-07-cli-run-general-runtime.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Prove CLI table-backed SQL run success

## Scope

Close the remaining CLI-spine parent gap for table-backed Postgres SQL resources by making `cdf run` execute a supported SQL resource through the general run spine from the CLI surface, with direct CLI success-path evidence.

Owns:

- `crates/cdf-cli/src/run_command.rs` and adjacent CLI runtime-dependency assembly if needed.
- `crates/cdf-cli/src/tests.rs` focused CLI SQL-run tests.
- CLI-facing project/runtime adapter changes only if the existing lower-layer SQL runtime dependencies cannot be supplied from the CLI without widening semantics.

## Acceptance criteria

- A table-backed declarative Postgres SQL resource with a ratified ordered cursor runs successfully through `cdf run` from the CLI.
- The run goes through `cdf_project::run_project`, writes a package, records run-ledger events, records a destination receipt, commits the checkpoint through `CheckpointStore::commit`, and emits stable JSON fields consistent with other `run` reports.
- The test proves source credentials are resolved through secret references without leaking resolved DSNs.
- Existing fail-closed SQL cases for missing secret and unsupported cursor/query shapes remain intact.

## Evidence expectations

Run focused CLI SQL-run success and failure tests, relevant `cdf-project` SQL runtime tests, `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`, `cargo check --workspace --all-targets --locked`, `git diff --check`, and applicable `QUALITY.md` security/duplication/complexity checks for touched files.

## Explicit exclusions

No arbitrary SQL query-resource execution beyond the already-ratified table-backed SQL resource slice, no new SQL dialects, no destination semantics changes, no scheduler/daemon work, and no lower-layer SQL runtime rewrite unless a concrete CLI integration blocker requires a narrow adapter.

## Design notes

Lower-layer table-backed Postgres SQL source execution is already closed under `.10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md` and `.10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md`. The remaining gap is product-facing CLI success-path wiring/evidence, not source engine semantics.

## Blockers

None known.

## Progress and notes

- 2026-07-07: Opened during CLI spine parent closure audit after resume closure. Audit found lower SQL runtime evidence but no direct CLI table-backed SQL `run` success-path test; existing CLI SQL tests prove only fail-closed missing secret and ordered-cursor validation behavior.
