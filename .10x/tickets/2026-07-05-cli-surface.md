Status: open
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/2026-07-05-datafusion-engine-planner.md, .10x/tickets/2026-07-05-checkpoint-store-sqlite.md

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

## Blockers

None.

