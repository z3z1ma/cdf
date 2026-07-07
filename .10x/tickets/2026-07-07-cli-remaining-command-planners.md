Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/checkpoint-state-commit-gate.md, .10x/specs/conformance-governance-roadmap.md

# Plan the remaining CLI command lower layers

## Scope

Own the non-run-spine CLI lower-layer gaps that prevent `.10x/tickets/2026-07-05-cli-surface.md` from closing after the run/resume/replay/inspect spine lands.

This is a planning parent, not an executable implementation ticket. Before implementation starts, split focused children for the independent command families below.

## Command families to split

- `cdf init`: project scaffold/write API and fixture-backed default project shape.
- `cdf plan` and `cdf explain`: scan/resource-schema to destination-DDL planning facade, including pushdown fidelity, delivery guarantee, and state-advancement preview.
- `cdf preview`: REST declarative preview, SQL declarative preview, Arrow IPC preview, and multi-file scan semantics without package, destination, or checkpoint writes.
- `cdf contract freeze` and `cdf contract test`: contract registry/snapshot writer, drift fixture runner, and fail-closed behavior for missing registry state.
- `cdf state migrate` and `cdf state recover`: explicit state migration runner, fixture-backed migration gates, and destination mirror recovery API.
- `cdf backfill`: bounded backfill planner/orchestrator with run-ledger ownership and checkpoint-safe slicing.
- `cdf package gc`: retention planner tied to checkpoint history and package manifest reachability.
- `cdf status`: integration of runtime-ledger/package receipt timestamps for freshness cases not covered by the local committed-head implementation.

## Acceptance criteria

- Each command family above has either a focused executable child ticket or a recorded no-action/supersession rationale.
- Child tickets reference the governing spec or decision that owns semantics before any implementation starts.
- CLI children preserve the rule that business logic stays in lower crates and CLI wiring does not bypass invariants.
- The parent CLI surface can use this ticket as the durable owner for non-run-spine lower-layer gaps.

## Evidence expectations

For this planning parent, evidence is record coherence: child tickets, supersession notes, or no-action rationale. Implementation children must define their own tests and quality gates.

## Explicit exclusions

No source edits, no command implementation, no CLI snapshots, no dependency changes, no generated fixtures, and no semantic defaults not already ratified by active specs or decisions.

## Blockers

None from user. This ticket exists to split remaining implementation work after the next goal is chosen.

## Progress and notes

- 2026-07-07: Opened while clearing stale CLI blocked status after user ratified the pending run-spine, DataFusion tuple, Postgres destination, and non-file checkpoint decisions. This ticket replaces a prose-only blocker list in `.10x/tickets/2026-07-05-cli-surface.md`.
