Status: open
Created: 2026-07-05
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md, .10x/tickets/2026-07-07-cli-remaining-command-planners.md

# Implement CLI surface

## Scope

Implement `cdf-cli` command parsing, JSON output mode, meaningful exit codes, project loading, and command plumbing for required MVP commands: init, validate, plan, explain, run, preview, sql, inspect, diff schema, contract freeze/show/test, state show/history/rewind/migrate/recover, resume, replay package, backfill, package ls/gc/verify, doctor, and status. Owns `crates/cdf-cli/**`.

## Acceptance criteria

- CLI command set matches `.10x/specs/project-cli-observability-security.md` except fast-follow `package archive`.
- `plan` and `explain` show pushdown fidelity, DDL preview, guarantee, and state advancement.
- `preview` inspects one batch and writes no package, destination data, or checkpoint.
- `run`, `resume`, and `replay package` route through package/destination/checkpoint invariants.
- `--json` emits stable structured output for automation-relevant commands.

## Evidence expectations

Record CLI integration tests, JSON snapshots, preview no-write tests, exit-code tests, and command help snapshots.

## Explicit exclusions

Business logic belongs in lower crates; CLI must not bypass lower-layer invariants.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to CLI worker. Worker owns `crates/cdf-cli/**`, its own evidence/review records, and may update `Cargo.lock` only for CLI dependencies. Do not touch `.gitignore`, lower-crate implementation, parent ticket, or unrelated records.
- 2026-07-06: Implemented the practical CLI surface in `crates/cdf-cli` with split modules for parsing, context loading, command handling, and JSON/error output. Commands use existing lower-crate APIs where exposed and return explicit unsupported exits instead of faking invariant-sensitive writes. Evidence recorded in `.10x/evidence/2026-07-06-cli-surface.md`.
- 2026-07-06: Implemented the first supported `cdf sql` surface under `.10x/tickets/done/2026-07-06-local-system-sql.md`: read-only local system-history queries over checkpoint and package metadata. `sql` is no longer a blocker for the CLI surface.
- 2026-07-06: Closed lower-layer child `.10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md`; explicit prepared-package DuckDB replay/recovery into `CheckpointStore::commit` now exists without source contact. At that point, CLI plumbing still waited on command-level project loading, explicit delta/receipt input handling, and broader run-ledger orchestration rather than on the lower-layer package-to-checkpoint primitive.
- 2026-07-06: Closed child `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`; `preview` now works for the first safe runtime slice: single-match declarative local file resources using the existing `cdf-formats::FileResource` execution path. Broader CLI acceptance is now tracked by the open dependency owners below.
- 2026-07-06: Closed child `.10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md` for the first live `run` slice: explicit declarative local file resource to DuckDB destination and SQLite checkpoint with package/destination/checkpoint invariants preserved. It intentionally requires explicit pipeline, target, package id, and checkpoint id inputs so this slice does not invent run-ledger defaults.
- 2026-07-07: Run-ledger and commit-session semantics were ratified in `.10x/decisions/run-ledger-commit-session-spine.md` and `.10x/specs/run-orchestration-ledger.md`. CLI run/resume/replay/inspect implementation is now owned by `.10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md` after lower-layer run-spine children complete.
- 2026-07-07: User ratified the remaining run-spine, Postgres destination, non-file checkpoint, and DataFusion tuple decisions. This parent is no longer a blocked semantic holder; remaining unsupported CLI surfaces are dependency-gated by focused open owners.

## Blockers

None from user or unresolved product semantics.

Full CLI acceptance is dependency-gated by open implementation owners:

- `run`, `resume`, `replay package`, and `inspect run` are owned by `.10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md`.
- `init`, DDL planning for `plan`/`explain`, broader `preview`, contract registry/fixture commands, state migration/recovery commands, backfill, package GC, and runtime-ledger freshness integration are owned by `.10x/tickets/2026-07-07-cli-remaining-command-planners.md`.

Verification note: `cargo fmt --all -- --check`, `cargo test -p cdf-cli --locked --no-fail-fast`, `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`, `cargo check -p cdf-cli --all-targets --locked`, and `cargo check --workspace --all-targets --locked` pass after parent integration. Semgrep's initial CLI argv/path findings were resolved by using a source-local test directory and documenting the intentionally non-security CLI argv dispatch boundary with a narrow `nosemgrep` comment. Full acceptance is now dependency-gated by the open owners listed above.
