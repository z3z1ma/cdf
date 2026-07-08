Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws6-docs-onboarding.md
Depends-On: .10x/specs/docs-onboarding-surface.md, .10x/tickets/done/2026-07-08-p1-product-ws2d-completions-manpages-help.md, .10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md, .10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md

# P1 product WS6B: Generated reference freshness

## Scope

Generate docs command reference from clap definitions and generate docs error reference from the WS4 error-code catalog, then wire freshness checks.

## Acceptance criteria

- `docs/commands/` is generated from the clap command definitions rather than hand-maintained syntax prose.
- `docs/errors/` is generated from the error-code catalog source of truth.
- A freshness check fails when generated command or error docs are stale.
- CI or local quality wiring records how the freshness check runs.

## Evidence expectations

Generator command output, stale-diff failure proof, and CI/local quality evidence.

## Explicit exclusions

No new parser grammar. No new error-code semantics. No release packaging; WS8 owns release artifacts.

## Progress and notes

- 2026-07-08: Unblocked after WS2D generated CLI artifacts and WS4B/WS4C established the CLI error catalog and suggestions source. WS4D depends on this generated reference slice, so this ticket must not depend on the WS4 parent as a whole.

## Blockers

None. WS2D, WS4B, and WS4C are complete.
