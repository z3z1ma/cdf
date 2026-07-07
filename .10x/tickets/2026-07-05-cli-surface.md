Status: blocked
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md

# Implement CLI surface

## Scope

Implement `firn-cli` command parsing, JSON output mode, meaningful exit codes, project loading, and command plumbing for required MVP commands: init, validate, plan, explain, run, preview, sql, inspect, diff schema, contract freeze/show/test, state show/history/rewind/migrate/recover, resume, replay package, backfill, package ls/gc/verify, doctor, and status. Owns `crates/firn-cli/**`.

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
- 2026-07-06: Assigned to CLI worker. Worker owns `crates/firn-cli/**`, its own evidence/review records, and may update `Cargo.lock` only for CLI dependencies. Do not touch `.gitignore`, lower-crate implementation, parent ticket, or unrelated records.
- 2026-07-06: Implemented the practical CLI surface in `crates/firn-cli` with split modules for parsing, context loading, command handling, and JSON/error output. Commands use existing lower-crate APIs where exposed and return explicit unsupported exits instead of faking invariant-sensitive writes. Evidence recorded in `.10x/evidence/2026-07-06-cli-surface.md`.
- 2026-07-06: Implemented the first supported `firn sql` surface under `.10x/tickets/done/2026-07-06-local-system-sql.md`: read-only local system-history queries over checkpoint and package metadata. `sql` is no longer a blocker for the CLI surface.
- 2026-07-06: Closed lower-layer child `.10x/tickets/done/2026-07-06-package-replay-firn-line-runtime.md`; explicit prepared-package DuckDB replay/recovery into `CheckpointStore::commit` now exists without source contact. CLI plumbing remains blocked on command-level project loading, explicit delta/receipt input handling, and broader run-ledger orchestration rather than on the lower-layer package-to-checkpoint primitive.
- 2026-07-06: Closed child `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`; `preview` now works for the first safe runtime slice: single-match declarative local file resources using the existing `firn-formats::FileResource` execution path. Broader CLI acceptance remains blocked on the lower-layer APIs listed below.
- 2026-07-06: Closed child `.10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md` for the first live `run` slice: explicit declarative local file resource to DuckDB destination and SQLite checkpoint with package/destination/checkpoint invariants preserved. It intentionally requires explicit pipeline, target, package id, and checkpoint id inputs so this slice does not invent run-ledger defaults.

## Blockers

Full acceptance is blocked by missing lower-layer APIs. Exact unsupported surfaces are recorded in `.10x/evidence/2026-07-06-cli-surface.md` and include:

- `init`: no project scaffold/write API.
- `plan`/`explain` DDL preview: no scan/resource-schema to destination-DDL planning facade.
- `preview`: the single-match local file resource slice is implemented in `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`. REST and SQL declarative preview runtime paths, Arrow IPC file preview, and multi-file scan semantics remain unsupported below the CLI.
- `run`: first explicit local file to DuckDB/SQLite orchestration slice is done in `.10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md`. Broader run-ledger defaults, automatic ids, REST/SQL resources, non-DuckDB destinations, and multi-resource runs remain blocked.
- `contract freeze` and `contract test`: no contract registry/snapshot writer or fixture runner.
- `state migrate` and `state recover`: no state migration runner or destination mirror recovery API.
- `resume`: no run ledger/recovery orchestrator for full run resumption; package-bound receipt recovery now has a lower-layer primitive in `.10x/tickets/done/2026-07-06-package-replay-firn-line-runtime.md`.
- `replay package`: package replay/checkpoint primitive now exists in `.10x/tickets/done/2026-07-06-package-replay-firn-line-runtime.md`, but CLI command wiring still has no ratified/project-backed way to load the explicit `StateDelta`, target, disposition, schema hash, merge keys, DuckDB destination, or supplied receipt inputs.
- `backfill`: no backfill planner/orchestrator.
- `package gc`: no retention planner tied to checkpoint history.
- `status` for resources with freshness SLOs: no runtime ledger/package receipt timestamps for freshness evaluation.

Verification note: `cargo fmt --all -- --check`, `cargo test -p firn-cli --locked --no-fail-fast`, `cargo clippy -p firn-cli --all-targets --locked -- -D warnings`, `cargo check -p firn-cli --all-targets --locked`, and `cargo check --workspace --all-targets --locked` pass after parent integration. Semgrep's initial CLI argv/path findings were resolved by using a source-local test directory and documenting the intentionally non-security CLI argv dispatch boundary with a narrow `nosemgrep` comment. Full acceptance remains blocked by the missing lower-layer APIs listed above.
