Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Depends-On: .10x/specs/datafusion-currency-bridges.md

# P3 J1: evidence statistics pruning

## Scope

Implement DataFusion `PruningStatistics` adapters over CDF file, segment, package, and profile evidence; compile recorded predicates into sound pruning decisions for replay, partial backfills, package SQL, and destination merge planning without opening skipped payloads.

## Acceptance criteria

- Missing/incompatible/stale statistics conservatively retain data.
- NULL, NaN, decimal, timezone, cast, nested, schema-evolution, and absent-stat cases are sound.
- Pruned and unpruned execution are row/verdict/commit equivalent for every supported predicate.
- Planner records predicate, evidence generation, skipped units/bytes, and conservative fallbacks.
- Pruning code lives in an engine adapter; package/stat artifacts expose no DataFusion types.

## Evidence expectations

Property/differential tests, corrupt/stale evidence adversaries, skipped-byte benchmarks, replay/backfill/sql/merge fixtures, dependency checks, and review.

## Explicit exclusions

No new statistics artifact schema unless separately ratified; no payload rewrite or package identity change.

## Blockers

None.

