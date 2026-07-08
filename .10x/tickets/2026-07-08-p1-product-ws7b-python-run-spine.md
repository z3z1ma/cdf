Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws7-python-front-door.md
Depends-On: .10x/specs/python-front-door-product-surface.md, .10x/tickets/2026-07-08-p1-product-ws7a-python-resource-resolution-plan-preview.md, .10x/tickets/done/2026-07-07-run-spine-implementation-program.md, .10x/tickets/done/2026-07-07-p0-workstream-b-open-orchestrator-world.md

# P1 product WS7B: Python resources through the run spine

## Scope

Make `cdf run <python-resource>` execute end to end through the general run spine.

Primary write scope is the Python resource adapter in `crates/cdf-project/src/runtime/**` or a focused sibling module, CLI run resolution in `crates/cdf-cli/src/project_run_resource.rs` and `crates/cdf-cli/src/run_command.rs`, focused tests, and this ticket's records. Preserve the existing split runtime modules; do not grow a monolithic `runtime.rs` or `commands.rs`.

## Acceptance criteria

- `cdf run <python-resource>` emits Python-produced Arrow batches into `run_project` through `ProjectRunSource` or its successor trait seam.
- Successful runs create valid packages, destination commits, trait-level receipt verification, checkpoint commits, run-ledger transitions, and replay/resume-compatible artifacts.
- Python resources work with the destinations supported by the general run spine unless a destination sheet legitimately excludes the cell.
- Resume after package finalization does not contact the Python source.
- Replay of a Python-produced package uses recorded package artifacts only and does not execute Python resource code.
- Duplicate commit handling remains observable and idempotent.
- Existing file, REST, and SQL run behavior and goldens remain unchanged unless a ratified artifact-version change is recorded.

## Evidence expectations

Record end-to-end run evidence, replay evidence, resume/no-source-contact evidence, duplicate handling evidence, destination matrix evidence, package verification, trait-level receipt verification, focused conformance coverage if a runtime path changed, and mandatory scoped quality checks from `QUALITY.md`, including CodeQL using a reusable database, jscpd, and complexity reports.

## Explicit exclusions

Do not implement plan/preview behavior unless WS7A left an explicitly recorded blocker. Do not implement dlt GA or interpreter CI matrix workflows. Do not add new destination drivers or source archetypes.

## Progress and notes

- 2026-07-08: Split from WS7 parent. This child depends on WS7A so run execution starts from a resolved, product-visible Python resource rather than a private bridge test.

## Blockers

Blocked until `.10x/tickets/2026-07-08-p1-product-ws7a-python-resource-resolution-plan-preview.md` is done.
