Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws3-rendering-system-design-language.md
Depends-On: .10x/specs/project-cli-observability-security.md

# P1 product WS3A: Ratify CLI design language decision

## Scope

Ratify the CLI design-language decision required before introducing the rendering layer.

Owns:

- Inventorying current human output paths and raw string renderers.
- Writing an active decision record in `.10x/decisions/` that selects the renderer stack, semantic palette, glyph/ASCII fallback system, table/panel primitives, humanization conventions, TTY/headless detection policy, redaction boundary, snapshot strategy, and no-new-command-on-old-path enforcement.
- Opening follow-up executable child tickets for renderer foundation and command migration after the decision is active.

## Acceptance criteria

- The decision record uses Nygard-style context, decision, alternatives considered, and consequences.
- The decision covers semantic colors, 8-color-safe fallback, truecolor/256-color enhancement posture, glyphs with ASCII fallbacks, tables, key-value panels, duration/byte/count humanization, next-command affordances, and section headers.
- The decision centralizes TTY, width, `NO_COLOR`, `CLICOLOR_FORCE`, and `--no-color` detection in one renderer boundary.
- The decision preserves the stable JSON contract and states that rendering cannot influence package artifacts, hashes, receipts, checkpoints, or goldens.
- The decision defines snapshot coverage for TTY and headless modes and the redaction adversarial check required before command migration closes.
- Renderer implementation child tickets are opened with disjoint write scopes.

## Evidence expectations

Record current output-path inventory, decision record, review of the decision, and no-code quality checks over the new/updated records.

## Explicit exclusions

No renderer implementation, no dependency changes, no command output changes, no snapshots in this ticket.

## Progress and notes

- 2026-07-08: Opened as the required design-language slice before WS3 renderer implementation. Dependency posture inspection found `cdf-cli` does not yet depend on renderer/parser crates, while the supply-chain config already contains exemptions for several mature CLI-rendering crates used elsewhere or anticipated by policy.
- 2026-07-08: Current output-path inventory recorded in `.10x/evidence/2026-07-08-p1-cli-inventory.md`; active renderer/design-language decision recorded in `.10x/decisions/cli-design-language-and-renderer.md`.
- 2026-07-08: Renderer migration children opened: `.10x/tickets/2026-07-08-p1-product-ws3b-renderer-foundation.md`, `.10x/tickets/2026-07-08-p1-product-ws3c-plan-run-rendering.md`, `.10x/tickets/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md`, and `.10x/tickets/2026-07-08-p1-product-ws3e-remaining-rendering-migration-gate.md`. Review recorded in `.10x/reviews/2026-07-08-cli-design-language-decision-review.md`. WS3A is closed as a decision slice.

## Blockers

None.
