Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws6-docs-onboarding.md
Depends-On: .10x/specs/docs-onboarding-surface.md

# P1 product WS6A: Docs topology and quickstart

## Scope

Create the `docs/` topology, architecture overview, operator-guide stubs with real scoped content where current behavior is implemented, and the first executable quickstart draft.

## Acceptance criteria

- `docs/quickstart.md` covers init, validate, plan, run, SQL, contract freeze/drift quarantine, crash/resume, replay, and inspection using only implemented or explicitly marked pending commands.
- `docs/architecture.md` links to `VISION.md` and active specs without duplicating the book.
- `docs/operators/` includes recovery, replay, backfill, doctor/status in cron, and release/install guide pages.
- The quickstart names documented prerequisites and does not require hidden local state.
- Any command/output snippets are verified against current CLI behavior or marked pending with a linked owning ticket.

## Evidence expectations

Docs tree diff, local link/content review, and quickstart accuracy evidence for all commands that are already implemented.

## Explicit exclusions

No generated command reference. No generated error catalog. No runnable examples beyond references to examples owned by WS6C. No docs site generator unless a later ticket scopes it.

## Blockers

Some final command/output snippets depend on WS2/WS3/WS4 rendering and grammar migration; this ticket may land a draft that links those blockers clearly.
