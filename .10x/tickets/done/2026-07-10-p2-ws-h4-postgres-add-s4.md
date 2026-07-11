Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md
Depends-On: .10x/decisions/cdf-add-dsn-secret-persistence.md, .10x/specs/data-onramp-source-experience-cli.md

# P2 WS-H4 Postgres add and S4 conformance

## Scope

Extend `cdf add` to direct Postgres table DSNs, persist direct credentials through the ratified private secret-file boundary, discover catalog schema, report cursor candidates without selecting them, and promote S4 with deterministic local-Postgres conformance.

## Acceptance criteria

- `cdf add warehouse.orders postgres://.../db/orders` writes runnable SQL resource TOML with no inline secret and pins catalog discovery.
- Direct DSNs persist only in owner-readable `.cdf/secrets/sources/<source>.dsn`; dry-run writes nothing.
- Reports redact userinfo and label serial/timestamp cursor candidates as suggestions; no key/cursor is silently chosen.
- The generated resource plans, previews, and runs after add.
- S4 is covered by deterministic local Postgres conformance.

## Blockers

None.

## Progress and notes

- 2026-07-10: Direct Postgres table DSNs now compile through a distinct add target, use an in-memory secret provider for no-write discovery, persist only an owner-readable private secret file on committed add, pin catalog schema, and report integer/timestamp/date cursor candidates as unselected suggestions. The deterministic local-Postgres test covers dry-run, secret redaction/mode, add, plan, preview, explicit cursor selection, and run. Evidence is `.10x/evidence/2026-07-10-p2-h4-postgres-add-s4.md`; review is `.10x/reviews/2026-07-10-p2-h4-postgres-add-s4-review.md`.
