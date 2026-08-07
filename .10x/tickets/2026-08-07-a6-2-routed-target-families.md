Status: open
Created: 2026-08-07
Updated: 2026-08-07
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/2026-08-07-a1-5-package-native-keyed-effects.md`

# A6.2: routed destination target families

## Scope

Implement `ROUTE BY <field> MAX TARGETS <n>` parsing/lowering, deterministic route tokens and
physical target derivation, protected route authority, package route partitions/identity,
multi-target destination capabilities/plans/receipts/replay, and PostgreSQL/DuckDB atomic family
application with plan/inspect/run evidence.

## Non-goals

- generated resources/checkpoints per route;
- null/default/overflow routes;
- sensitive route keys;
- inferred old-route deletes;
- destinations unable to prove package-atomic multi-target settlement.

## Acceptance criteria

- [ ] Grammar, manifest, and plan carry the route field, fold version, mandatory ceiling, and
      logical base target.
- [ ] Safe tokens remain exact; other admitted scalar values use deterministic slug-plus-hash
      tokens under destination identifier bounds with collision rejection.
- [ ] Routing is protected control authority and may be projected out only after route resolution.
- [ ] One package/receipt/checkpoint covers the canonical route map and per-target effects;
      partial/ambiguous settlement advances no checkpoint.
- [ ] PostgreSQL and DuckDB apply all physical targets atomically and replay idempotently.
- [ ] Standalone/shared-extraction execution produces identical package and route identities.
- [ ] Focused parser/package/destination/CLI and release sandbox behavior pass.

## References

- `.10x/specs/routed-destination-target-families.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/research/2026-08-07-routed-target-shared-extraction-readiness.md`

## Assumptions

All naming, privacy, cardinality, and settlement semantics are active, user-ratified authority.

## Journal

- 2026-08-07: Opened after ratification. One logical target remains authority; physical route
  tables are a bounded destination family and never implicit resources.

## Blockers

Dependency A1.5 must close before keyed CDC route application. Ordinary-row route identity shares
the same package/destination model and must not become a parallel implementation.

## Evidence

Pending implementation.

## Review

Pending tranche-level review.

## Retrospective

Pending implementation.
