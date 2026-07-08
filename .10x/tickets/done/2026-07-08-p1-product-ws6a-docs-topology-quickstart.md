Status: done
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

None.

## Progress and notes

- 2026-07-08: Created the initial `docs/` topology, quickstart, architecture overview, generated command/error placeholder indexes, and operator guides for recovery, replay, backfill, doctor/status in cron, release/install, and troubleshooting. Generated command reference and error catalog remain owned by WS6B; runnable examples remain owned by WS6C; init README remains owned by WS6D.
- 2026-07-08: Verified current quickstart commands against the rebuilt CLI in temporary projects, including init, validate, plan, run, system-history SQL, package/state inspection, contract freeze/test, and clean-ledger replay. Verified crash/resume and drift-quarantine docs against the conformance MVP fixture. Evidence: `.10x/evidence/2026-07-08-p1-product-ws6a-docs-topology-quickstart.md`.
- 2026-07-08: Closure review passed with residual risk limited to out-of-scope generated docs/examples/init/release work and unrelated concurrent CLI formatting dirt. Review: `.10x/reviews/2026-07-08-p1-product-ws6a-docs-topology-quickstart-review.md`.
