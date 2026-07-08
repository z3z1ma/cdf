Status: done
Created: 2026-07-05
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md, .10x/tickets/done/2026-07-07-cli-remaining-command-planners.md, .10x/tickets/done/2026-07-07-cli-init-scaffold.md, .10x/tickets/done/2026-07-07-cli-plan-explain-ddl-guarantee.md, .10x/tickets/done/2026-07-07-cli-status-runtime-ledger-freshness.md, .10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md, .10x/tickets/done/2026-07-07-cli-contract-registry-freeze-test.md, .10x/tickets/done/2026-07-07-cli-state-migrate-recover.md, .10x/tickets/done/2026-07-07-cli-backfill-planner.md, .10x/tickets/done/2026-07-07-cli-package-gc-retention.md

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
- 2026-07-07: Run-ledger and commit-session semantics were ratified in `.10x/decisions/run-ledger-commit-session-spine.md` and `.10x/specs/run-orchestration-ledger.md`. CLI run/resume/replay/inspect implementation is now owned by `.10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md` after lower-layer run-spine children complete.
- 2026-07-07: User ratified the remaining run-spine, Postgres destination, non-file checkpoint, and DataFusion tuple decisions. This parent is no longer a blocked semantic holder; remaining unsupported CLI surfaces are dependency-gated by focused open owners.
- 2026-07-08: Closed `.10x/tickets/done/2026-07-07-cli-plan-explain-ddl-guarantee.md`; `cdf plan` and `cdf explain` now require a target and expose no-write resource schema, destination sheet, DDL/migration preview, derived delivery guarantee, and state-advancement output through the lower project planning facade.
- 2026-07-08: Closed `.10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md`; `cdf preview` now covers REST, table-backed SQL, Arrow IPC, and deterministic first-match multi-file preview with no-write proof, direct-stream residual fail-closed behavior, and required JSON fields.
- 2026-07-08: Closed `.10x/tickets/done/2026-07-07-cli-init-scaffold.md`; `cdf init [DIR] [--name NAME] [--force]` now creates the ratified minimal local project scaffold through `cdf-project`, validates without manual edits, emits stable human/JSON output, and preserves runtime artifacts/user data under the recorded overwrite policy.
- 2026-07-08: Closed `.10x/tickets/done/2026-07-07-cli-status-runtime-ledger-freshness.md`; `cdf status` now reports runtime-ledger/package-receipt freshness observations while preserving committed checkpoint-head authority.
- 2026-07-08: Closed `.10x/tickets/done/2026-07-07-cli-package-gc-retention.md`; `cdf package gc [DIR]` now produces a dry-run retention plan that classifies retained, collectible, missing, corrupt, and protected package artifacts from package manifests, receipts, tombstones, and read-only committed checkpoint history.
- 2026-07-08: Closed `.10x/tickets/done/2026-07-07-cli-contract-registry-freeze-test.md`; `cdf contract freeze` and `cdf contract test` now operate over deterministic `cdf.lock` snapshots with missing-registry fail-closed behavior, drift details, and project-free `contract show` preserved.
- 2026-07-08: Closed `.10x/tickets/done/2026-07-07-cli-state-migrate-recover.md`; `cdf state migrate` now reports/idempotently initializes SQLite state component versions, and `cdf state recover --package --to [--receipt]` recovers checkpoint state from a verified package receipt without destination row writes or a direct checkpoint-head bypass.
- 2026-07-08: Closed `.10x/tickets/done/2026-07-07-cli-backfill-planner.md`; `cdf backfill RESOURCE --from CURSOR --to CURSOR --target TARGET [--execute] [--slice-size N]` now dry-plans bounded cursor windows by default and executes eligible slices through `run_project` with concrete window checkpoint scopes.
- 2026-07-08: Closed this parent with aggregate evidence `.10x/evidence/2026-07-08-cli-surface-closure.md` and review `.10x/reviews/2026-07-08-cli-surface-closure-review.md`. All command-family child owners are done; remaining full-system work belongs to other active parent lanes.

## Blockers

None. All command-family child owners are closed.

Verification note: the final backfill closure reran focused CLI/project tests, full workspace tests, fmt/clippy, `jscpd`, rust-code-analysis, supply-chain/security scans, source-only Gitleaks, and reusable-DB CodeQL. Earlier parent integration checks and child evidence remain linked above; no CLI command-family dependency remains open under this parent.
