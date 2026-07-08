Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-05-cli-surface.md, .10x/specs/project-cli-observability-security.md

# CLI surface closure

## What was observed

The CLI surface parent is no longer dependency-gated by an open command-family owner. The final open child, `.10x/tickets/done/2026-07-07-cli-backfill-planner.md`, now has evidence and review for bounded dry planning and execution through the run spine.

The parent surface has done children for init, validate/project loading, plan/explain DDL and guarantee output, run/resume/replay/inspect-run through the run spine, direct SQL-run success, preview breadth, `cdf sql`, diff-schema/scan-style inspection, contract freeze/show/test, state show/history/rewind/migrate/recover, package ls/gc/verify/archive, doctor, status, and backfill.

## Procedure

- Re-read `.10x/tickets/done/2026-07-05-cli-surface.md` acceptance criteria.
- Reconciled its dependency list against closed children, including the final backfill child.
- Verified the final child evidence `.10x/evidence/2026-07-08-cli-backfill-planner.md` covers the remaining command-family acceptance surface with focused behavior tests and broad quality gates.
- Updated `.10x/knowledge/vision-coverage-matrix.md` to mark Chapter 18 CLI coverage done while keeping broader product/demo rows active where appropriate.

## What this supports

- The CLI parent can close as an aggregate orchestration record.
- `VISION.md` Chapter 18's required headless CLI surface is now represented by active source plus done child evidence/review records.
- The full-system parent remains active because conformance/demo, observability, WASM, CDC/streaming, distributed, lakehouse/warehouse/vault, and other full CDF 1.0 lanes are still open.

## Limits

This evidence does not claim the whole CDF system is complete. It only supports closing the CLI-surface parent after its children are done.
