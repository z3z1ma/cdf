Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/specs/conformance-governance-roadmap.md, .10x/tickets/2026-07-08-p1-product-ws2-command-grammar-redesign.md, .10x/tickets/2026-07-08-p1-product-ws4-error-experience-catalog.md

# P1 product WS6: Documentation and onboarding

## Scope

Build an enterprise-credible in-repository documentation and onboarding surface: quickstart, generated command reference, error catalog, operator guides, architecture overview, runnable examples, and init-scaffold links.

This workstream may start with docs structure and examples, but command-reference and error-catalog closure depend on WS2 and WS4.

## Required outcomes

- `docs/` contains a quickstart covering init, plan, run, drift, resume, and replay.
- Per-command reference is generated from clap definitions and diff-checked in CI.
- The error-code catalog is generated into docs.
- Operator guides cover state recovery, replay, backfill, doctor/status in cron, and release/install basics.
- Architecture overview links to `VISION.md` rather than duplicating the book.
- `examples/` contains at least REST-fixture and Postgres sample projects runnable by conformance.
- `cdf init` scaffolds a project whose README points to the quickstart.
- Completions and man pages ship as build/release artifacts.

## Acceptance criteria

- Docs build in CI.
- Doc freshness checks fail when generated command reference or error catalog drift.
- Example projects execute as living tests.
- A stranger can execute the quickstart from a clean checkout without hidden state beyond documented prerequisites.

## Evidence expectations

Record docs build output, generated-reference freshness output, example execution output, conformance integration evidence, and review of quickstart accuracy.

## Explicit exclusions

No external docs site requirement. No marketing page. No duplication of full `VISION.md` content.

## Progress and notes

- 2026-07-08: Opened from P1 product directive. This lane may start immediately but has explicit closure dependencies on grammar and error-catalog generation.
- 2026-07-08: Ratified `.10x/specs/docs-onboarding-surface.md` and split execution into `.10x/tickets/2026-07-08-p1-product-ws6a-docs-topology-quickstart.md`, `.10x/tickets/2026-07-08-p1-product-ws6b-generated-reference-freshness.md`, `.10x/tickets/2026-07-08-p1-product-ws6c-runnable-examples-conformance.md`, and `.10x/tickets/2026-07-08-p1-product-ws6d-init-readme-scaffold.md`.

## Blockers

Closure depends on WS2 generated command definitions and WS4 generated error catalog.
