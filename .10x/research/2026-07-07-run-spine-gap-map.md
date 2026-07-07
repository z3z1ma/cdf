Status: done
Created: 2026-07-07
Updated: 2026-07-07

# Run spine gap map

## Question

What high-leverage architectural gap most directly blocks broader `cdf run`, `resume`, `replay package`, `inspect run`, multi-destination orchestration, and MVP killer-demo completion?

## Sources and methods

- User-provided architecture audit in the 2026-07-07 goal continuation thread.
- `VISION.md` Chapter 23 MVP and demonstration requirements.
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/tickets/2026-07-05-cli-surface.md`
- `.10x/tickets/2026-07-05-observability-doctor-status-sql.md`
- Source inspection:
  - `crates/cdf-project/src/runtime.rs`
  - `crates/cdf-kernel/src/destination.rs`
  - `crates/cdf-cli/src/commands.rs`

## Findings

- The current project runtime has correct but specialized orchestration functions, including `run_local_file_to_duckdb_checkpoint`, `replay_duckdb_package_from_artifacts`, and `recover_prepared_duckdb_package`. These prove package/checkpoint/receipt invariants for the local DuckDB slice, but they are not a general run runtime.
- `DestinationProtocol` currently exposes `sheet()` and `plan_commit()` only. It does not define a driver-neutral commit session abstraction for beginning, writing/finalizing, verifying, and aborting package commits.
- `cdf run` still rejects REST and SQL resources and non-local-DuckDB destinations in the CLI/project runtime path, even though lower-level REST execution and destination implementations exist or are emerging.
- `inspect run` is explicitly blocked by unratified run-id minting, run-ledger ownership, run-to-package/checkpoint/receipt mapping, multi-resource/multi-package bounds, transition ordering storage, and verdict-summary ownership.
- These gaps are related: without a ratified run model and driver-neutral commit session, new source/destination support keeps landing as vertical slices that cannot compose into the book-required general run and killer-demo path.

## Conclusions

The highest-leverage next architectural shaping item is the run spine: ratify run-ledger semantics, define a kernel/project commit-session abstraction, and then open executable implementation tickets to generalize runtime orchestration while preserving the existing DuckDB/file wrappers as compatibility facades.

This research does not itself ratify the exact run-ledger contract. The unresolved semantics are owned by `.10x/tickets/2026-07-07-run-ledger-commit-session-spine-ratification.md`.
