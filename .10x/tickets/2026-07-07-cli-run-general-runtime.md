Status: blocked
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md
Depends-On: .10x/tickets/done/2026-07-07-general-run-orchestrator.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Wire `cdf run` to the general runtime

## Scope

Replace the DuckDB/local-file-only CLI `run` implementation with routing through `cdf_project::run_project`.

Owns:

- `crates/cdf-cli/src/commands.rs`
- `crates/cdf-cli/src/context.rs`
- `crates/cdf-cli/src/args.rs` only if run-id or destination options need parser support.
- Focused CLI tests for supported and unsupported run combinations.

## Acceptance criteria

- `cdf run` routes local file, exact zero-lag REST, and table-backed Postgres SQL resources through `ProjectRunRequest`/`run_project` where the lower-layer resource runtime dependencies are available.
- `cdf run` supports DuckDB and Postgres environment destinations and supports filesystem Parquet once a CLI URI spelling is ratified in this ticket.
- Unsupported resource/destination/disposition combinations fail before source, package, destination, or checkpoint mutation.
- JSON output includes run id, package, checkpoint, receipt, destination, receipt source, row/segment counts, and ledger event summary.
- Existing DuckDB/local-file CLI run behavior remains compatible except for gaining run id/ledger fields.

## Evidence expectations

Run focused CLI run tests, relevant `cdf-project` tests, `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`, `cargo check --workspace --all-targets --locked`, and `git diff --check`.

## Explicit exclusions

No `resume`, no `replay package`, no `inspect run`, no `run --loop`, no arbitrary SQL query execution, no new destination semantics beyond CLI destination URI/config parsing needed for already-supported destinations.

## Design notes

- Current CLI `run` uses `run_local_file_to_duckdb_checkpoint` and rejects REST/SQL/Postgres/Parquet because the runtime used to be specialized.
- Existing project config has `duckdb://` and `postgres://` examples. No active record names the filesystem Parquet CLI URI; if implementation needs it, prefer a small recorded decision or explicit blocker before adding user-visible syntax. Candidate spelling is `parquet://path`, mirroring `duckdb://path`.
- Postgres destination credentials must resolve through the existing secret provider without serializing resolved values into reports.

## Blockers

- REST CLI run routing is blocked by the absence of a production `HttpTransport` adapter in the current crates. `cdf-project` supports dependency-bearing REST resources, but `cdf-cli` has no production transport to supply without expanding lower-layer runtime semantics.
- Postgres CLI destination routing is blocked by `.10x/decisions/project-run-postgres-destination-inputs.md`: the active decision requires explicit destination/run configuration for existing-table policy and merge dedup policy, and no active CLI/project configuration syntax currently supplies those values.
- Filesystem Parquet CLI URI spelling must be ratified inside this ticket before enabling Parquet CLI run support.

## Progress and notes

- 2026-07-07: Split from the broad CLI spine ticket after general orchestrator closure.
- 2026-07-07: Wired `cdf run` through `cdf_project::run_project(ProjectRunRequest)` for local file resources, table-backed SQL resource adapters, and DuckDB destinations. JSON run reports now include the minted run id, destination summary, receipt object, row/segment counts, and run-ledger event summary while preserving existing DuckDB/local-file fields.
- 2026-07-07: Kept REST fail-closed before run mutation because no production `HttpTransport` exists in the current CLI/lower-layer surface. Kept Postgres destination fail-closed because active decision `.10x/decisions/project-run-postgres-destination-inputs.md` requires explicit existing-table and merge-dedup policy configuration that the CLI/project config does not yet provide. Kept filesystem Parquet fail-closed because no active record ratifies a CLI URI spelling.
- 2026-07-07: Focused evidence recorded in `.10x/evidence/2026-07-07-cli-run-general-runtime.md`.
