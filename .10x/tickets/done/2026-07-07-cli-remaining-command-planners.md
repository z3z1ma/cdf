Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/checkpoint-state-commit-gate.md, .10x/specs/conformance-governance-roadmap.md

# Plan the remaining CLI command lower layers

## Scope

Own the non-run-spine CLI lower-layer gaps that prevent `.10x/tickets/2026-07-05-cli-surface.md` from closing after the run/resume/replay/inspect spine lands.

This is a planning parent, not an executable implementation ticket. Before implementation starts, split focused children for the independent command families below.

## Command families to split

- Cross-cutting command module architecture: done in `.10x/tickets/done/2026-07-07-cli-command-module-architecture.md`.
- `cdf init`: `.10x/tickets/done/2026-07-07-cli-init-scaffold.md`.
- `cdf plan` and `cdf explain`: `.10x/tickets/done/2026-07-07-cli-plan-explain-ddl-guarantee.md`.
- `cdf preview`: `.10x/tickets/2026-07-07-cli-preview-resource-breadth.md`.
- `cdf contract freeze` and `cdf contract test`: `.10x/tickets/2026-07-07-cli-contract-registry-freeze-test.md`.
- `cdf state migrate` and `cdf state recover`: `.10x/tickets/2026-07-07-cli-state-migrate-recover.md`.
- `cdf backfill`: `.10x/tickets/2026-07-07-cli-backfill-planner.md`.
- `cdf package gc`: `.10x/tickets/done/2026-07-07-cli-package-gc-retention.md`.
- `cdf status`: `.10x/tickets/done/2026-07-07-cli-status-runtime-ledger-freshness.md`.

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

None.

## Evidence

- `.10x/evidence/2026-07-07-cli-remaining-command-planner-split.md`

## Review

- `.10x/reviews/2026-07-07-cli-remaining-command-planner-split-review.md`

## Progress and notes

- 2026-07-07: Opened while clearing stale CLI blocked status after user ratified the pending run-spine, DataFusion tuple, Postgres destination, and non-file checkpoint decisions. This ticket replaces a prose-only blocker list in `.10x/tickets/2026-07-05-cli-surface.md`.
- 2026-07-07: Added executable architecture child, now closed as `.10x/tickets/done/2026-07-07-cli-command-module-architecture.md`, after `cdf run` extraction metrics showed `commands.rs` remains too broad even though the run path moved out.
- 2026-07-07: Closed architecture child as `.10x/tickets/done/2026-07-07-cli-command-module-architecture.md`; remaining command-family bullets are still lower-layer behavior/planner work.
- 2026-07-07: Split all remaining command families into direct children of `.10x/tickets/2026-07-05-cli-surface.md`. Evidence recorded in `.10x/evidence/2026-07-07-cli-remaining-command-planner-split.md`; review passed in `.10x/reviews/2026-07-07-cli-remaining-command-planner-split-review.md`.
