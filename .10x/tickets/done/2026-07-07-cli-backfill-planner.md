Status: done
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/checkpoint-state-commit-gate.md, .10x/decisions/backfill-window-planner-command-contract.md

# Implement cdf backfill planner

## Scope

Implement bounded `cdf backfill [RESOURCE]` planning and execution through the general run spine.

Owns:

- `crates/cdf-cli/src/backfill_command.rs` and focused tests.
- Lower `cdf-project` backfill planner/orchestrator APIs needed to create checkpoint-safe historical slices.
- Run-ledger ownership and reporting for backfill-created runs.

## Acceptance criteria

- `cdf backfill` builds explicit bounded windows/slices for eligible resources and rejects resources without ratified backfill semantics.
- Each executed slice routes through the general run spine and records run-ledger events, packages, receipts, and checkpoint commits normally.
- Backfill never rewinds or advances state outside `CheckpointStore::commit`.
- JSON output includes planned slices, executed runs, checkpoint ids, package pointers, and skipped/unsupported reasons.
- Dry planning versus execution behavior is ratified before implementation if a new flag or mode is required.

## Evidence expectations

Run focused CLI backfill tests, no-unsupported-source-contact tests where applicable, run-ledger/checkpoint assertions, relevant project runtime tests, fmt/clippy/check/diff checks, and applicable quality scans.

## Explicit exclusions

No scheduler, no resident loop, no distributed leases, no CDC/log backfill, no unbounded streams, and no source-specific shortcut outside the general run spine.

## Blockers

None. If CLI mode/flag semantics are not ratified enough, self-ratify the narrow command contract before source edits.

## Progress and notes

- 2026-07-07: Split from `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`. Current CLI validates the optional resource id then returns not-supported because bounded historical planning is not exposed by lower crates.
- 2026-07-08: Ratified the first backfill command contract in `.10x/decisions/backfill-window-planner-command-contract.md`: default dry planning, explicit `--execute`, required `--target`, half-open cursor slices, numeric `--slice-size` only, concrete `ScopeKey::Window` per executed slice, and `cdf-project` ownership of slice planning.
- 2026-07-08: Implemented first `cdf backfill` planner/executor slice. `cdf-project` now owns cursor-window planning, deterministic package/checkpoint ids, exact predicate eligibility, and a window-scoped resource wrapper. `cdf-cli` parses the ratified command shape, reports dry plans without runtime writes, and routes `--execute` slices through `run_project`.
- 2026-07-08: Closed with evidence `.10x/evidence/2026-07-08-cli-backfill-planner.md` and review `.10x/reviews/2026-07-08-cli-backfill-planner-review.md`. Focused backfill tests, live SQL-source execution through DuckDB, workspace tests, fmt/clippy, `jscpd`, rust-code-analysis, supply-chain/security scans, source-only Gitleaks, and reusable-DB CodeQL passed or matched ratified residuals.
