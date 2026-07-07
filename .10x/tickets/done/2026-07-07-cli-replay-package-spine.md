Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md
Depends-On: .10x/tickets/done/2026-07-07-general-run-orchestrator.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md, .10x/decisions/destination-introspection-package-and-cli-policy.md

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
- Postgres replay requires explicit `--target` and `--merge-dedup` CLI inputs for semantics package artifacts do not own, and fails closed before mutation when those inputs are absent, inconsistent, or conflict with the package destination-commit target.
- Duplicate package-token receipts are represented in JSON/human output when the destination exposes duplicate/no-op status.
- Replay JSON includes package hash, destination id, target, receipt id, checkpoint id/status, receipt source, duplicate status when known, and package status.

## Evidence expectations

Run focused replay CLI tests for DuckDB, Parquet, Postgres fail-closed policy, and duplicate replay where deterministic; run relevant `cdf-project` artifact replay tests, clippy for CLI/project, workspace check, and `git diff --check`.

## Explicit exclusions

No `resume`, no `inspect run`, no new package artifact schema, no source extraction, no destination introspection as a source of missing Postgres semantics.

## Design notes

- Current parser accepts only `replay package <DIR>` and the command returns not-supported.
- `cdf_project` now exposes DuckDB, Parquet, and Postgres artifact replay functions. Postgres replay intentionally requires explicit target and merge-dedup input by `.10x/decisions/destination-introspection-package-and-cli-policy.md`.
- `.10x/decisions/destination-introspection-package-and-cli-policy.md` ratifies `parquet://<root>` as a filesystem Parquet destination root/prefix, not a single file.
- The command currently does not receive `Cli`; implementation likely needs dispatch to call `replay_package(&cli, args)` so it can use selected project environment state and secret providers.

## Blockers

None.

## Progress and notes

- 2026-07-07: Split from the broad CLI spine ticket after package-artifact replay became available for all current project-run destinations.
- 2026-07-07: Implemented the DuckDB-only CLI replay slice. `cdf replay package <DIR> --to duckdb://path` now parses, loads the selected environment state store, replays package artifacts through `cdf_project::replay_duckdb_package_from_artifacts`, commits the checkpoint, records a `replay_recorded` run-ledger event, and reports package hash, destination id, target, receipt id, checkpoint id/status, receipt source duplicate/no-op status, and package status. At implementation time, the CLI failed closed before replay mutation for missing `--to`, Postgres policy inputs, then-unratified Parquet URI spelling, and unknown destination schemes. Evidence: `.10x/evidence/2026-07-07-cli-duckdb-package-replay.md`.
- 2026-07-07: Parent review added a missing-package non-mutation regression and reran focused quality gates. The DuckDB slice is acceptable progress. At review time, Parquet URI spelling and Postgres replay CLI policy inputs were still unratified. Review: `.10x/reviews/2026-07-07-cli-duckdb-package-replay-review.md`.
- 2026-07-07: User ratified `.10x/decisions/destination-introspection-package-and-cli-policy.md`: `parquet://<root>` is the filesystem Parquet destination root/prefix; destination introspection is standard wherever applicable but cannot infer missing write semantics; package scope is one resource transition; and Postgres replay uses explicit `--target` and `--merge-dedup fail`. Decision-level blockers are cleared; implementation wiring remains.
- 2026-07-07: Implemented the Parquet portion only. `cdf replay package <pkg> --to parquet://<root>` now parses the filesystem root/prefix, replays package artifacts through `cdf_project::replay_parquet_package_from_artifacts`, records `replay_recorded`, and reports destination kind/id/root/target, receipt, checkpoint, package status, and run-ledger summary in JSON. Empty and nested/non-filesystem Parquet URIs fail closed before state, package, or destination mutation. Postgres replay policy parsing remains excluded. Focused success and malformed-URI CLI tests were updated. Evidence: `.10x/evidence/2026-07-07-cli-parquet-run-replay.md`. Review: `.10x/reviews/2026-07-07-cli-parquet-run-replay-review.md`.
- 2026-07-07: Implemented the Postgres portion and closed the child. `cdf replay package <pkg> --to postgres://... --target schema.table --merge-dedup fail` now resolves plain or secret-backed Postgres destinations, validates the explicit target against the package destination commit target before mutation, replays through `cdf_project::replay_postgres_package_from_artifacts`, records `replay_recorded`, commits checkpoint state, appends a package receipt, and reports the required JSON fields. Missing target, missing merge-dedup, unsupported merge-dedup, and target mismatch fail closed before replay mutation; secret-backed target mismatch redacts the resolved DSN. Evidence: `.10x/evidence/2026-07-07-cli-postgres-package-replay.md`. Review: `.10x/reviews/2026-07-07-cli-postgres-package-replay-review.md`.
