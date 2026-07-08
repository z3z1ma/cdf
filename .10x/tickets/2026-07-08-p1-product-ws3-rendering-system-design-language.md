Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/specs/project-cli-observability-security.md

# P1 product WS3: Rendering system and design language

## Scope

Create a dedicated human-rendering layer that every command uses, and ratify the CLI design language before implementation.

This workstream is likely to split into decision, renderer foundation, command migration, and snapshot child tickets. The design decision MUST land before code adopts the renderer.

## Required outcomes

- Ratify a design-language decision covering semantic color palette, 8-color-safe fallback with richer enhancement, typography, status glyphs with ASCII fallbacks, box drawing, table rendering, key-value panels, duration/byte/count humanization, section headers, and next-command affordances.
- Implement a dedicated renderer module or crate. TTY, width, `NO_COLOR`, `CLICOLOR_FORCE`, and `--no-color` detection live in the renderer and nowhere else.
- Every command's human output migrates to the renderer.
- TTY and piped modes have snapshots.
- Redaction applies before output leaves the renderer.
- No new command lands on raw `human: String` formatting after this workstream merges.

## Acceptance criteria

- `cdf plan` and `cdf run` human output match or exceed the density and operator usefulness of the P1 directive mockups.
- Tables truncate width-aware without corrupting ANSI or hiding critical failure context.
- Headless mode emits readable non-ANSI output.
- A migration checklist proves no command remains on the old raw human-formatting path.

## Evidence expectations

Record the design-language decision, renderer tests, TTY and non-TTY insta snapshots for every command, redaction adversarial checks, and the migration checklist.

## Explicit exclusions

No live progress implementation except renderer primitives; WS5 owns event consumption and interactive progress. No JSON contract changes.

## Progress and notes

- 2026-07-08: Opened from P1 product directive. This workstream is the quality bar for the CLI face of the system.
- 2026-07-08: Split decision child `.10x/tickets/done/2026-07-08-p1-product-ws3a-cli-design-language-decision.md` before renderer implementation.
- 2026-07-08: Active renderer/design-language decision recorded in `.10x/decisions/cli-design-language-and-renderer.md`; implementation children split as WS3B renderer foundation, WS3C plan/run/replay rendering, WS3D recovery/state/backfill rendering, and WS3E remaining rendering migration gate.
- 2026-07-08: WS3B renderer foundation closed at `.10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md`. WS3C plan/run/replay rendering closed at `.10x/tickets/done/2026-07-08-p1-product-ws3c-plan-run-rendering.md`; plan/explain next commands preserve explicit `--to` destinations without user-minted package/checkpoint identifiers.
- 2026-07-08: WS3D recovery/state/backfill rendering closed at `.10x/tickets/done/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md`. WS3E remaining rendering migration gate closed at `.10x/tickets/done/2026-07-08-p1-product-ws3e-remaining-rendering-migration-gate.md`; parent closure still needs aggregate evidence/review before moving this workstream to done.

## Blockers

None for remaining child execution. Parent closure still needs aggregate evidence/review and coverage-matrix reconciliation.
