Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws7-python-front-door.md
Depends-On: .10x/specs/python-front-door-product-surface.md, .10x/tickets/done/2026-07-05-dlt-shim-preview.md, .10x/tickets/2026-07-08-p1-product-ws7a-python-resource-resolution-plan-preview.md

# P1 product WS7D: dlt GA gap integration

## Scope

Move the dlt shim from preview toward GA by proving a real dlt resource/source integration through CDF semantics or by ratifying an explicit remaining gap list with owners and sequence.

Primary write scope is dlt-specific modules in `crates/cdf-python/src/**`, Python SDK dlt helpers in `python/cdf_sdk/**`, representative integration fixtures, focused tests, and this ticket's records.

## Acceptance criteria

- A real or realistically imported `@dlt.resource` and `@dlt.source` path maps primary key, merge key, incremental, write disposition, contract-mode hints, selected/skipped resources, and scoped state into CDF descriptors, contracts, and positions.
- The integration path emits CDF batches that can feed preview and, once WS7B exists, package/run semantics without delegating destination writes to dlt.
- Divergences from dlt behavior are captured as compatibility data suitable for generated docs.
- If full GA parity is not safe in this slice, the remaining gaps are explicit, sequenced, and owned by tickets rather than prose-only notes.
- Existing preview fixture behavior remains covered.

## Evidence expectations

Record dlt integration tests or an explicit gap-list evidence record, mapping snapshots, state view tests, compatibility divergence snapshots, and mandatory scoped quality checks from `QUALITY.md`, including jscpd and complexity reports for touched Rust/Python.

## Explicit exclusions

Do not delegate destination commits to dlt. Do not implement non-dlt Python resource resolution unless WS7A leaves a blocker. Do not add new destination drivers or scheduler behavior.

## Progress and notes

- 2026-07-08: Split from WS7 parent. The existing `.10x/tickets/done/2026-07-05-dlt-shim-preview.md` proves preview shim mechanics but explicitly excludes full GA parity.

## Blockers

Blocked on `.10x/tickets/2026-07-08-p1-product-ws7a-python-resource-resolution-plan-preview.md` for product-level preview wiring. It may still perform read-only investigation before that dependency closes.
