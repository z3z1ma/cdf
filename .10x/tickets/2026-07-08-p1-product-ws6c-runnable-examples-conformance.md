Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws6-docs-onboarding.md
Depends-On: .10x/specs/docs-onboarding-surface.md

# P1 product WS6C: Runnable examples and conformance hooks

## Scope

Add REST-fixture and Postgres example projects under `examples/` and wire them into conformance or equivalent living tests.

## Acceptance criteria

- `examples/rest-fixture/` contains a runnable project with local fixtures and exact README commands.
- `examples/postgres/` contains a runnable project with documented local Postgres setup or existing test harness reuse.
- Both examples avoid real credentials and use `secret://` references where secrets are discussed.
- Conformance or an equivalent test target executes the examples as living tests.
- The examples are referenced from `docs/quickstart.md` or operator docs.

## Evidence expectations

Example execution output, conformance/test output, and redaction/secret reference review.

## Explicit exclusions

No external SaaS dependency. No new source archetype. No production Postgres provisioning.

## Blockers

None for fixture example creation. Postgres execution may depend on local harness availability in CI.
