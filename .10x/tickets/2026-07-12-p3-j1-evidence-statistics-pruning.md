Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Depends-On: .10x/specs/datafusion-currency-bridges.md, .10x/tickets/2026-07-12-p3-j0-typed-statistics-evidence-spine.md

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

J0 must provide sound typed, manifest-bound evidence. The 2026-07-12 readiness audit found current lexical `BatchStats` unpopulated and current `profile.json` aggregate-only; adapting either would be unsound or useless.

## Progress and notes

- 2026-07-12: Readiness audit corrected the initial premise that per-column/per-segment typed evidence already existed. J0 now owns the missing neutral evidence spine; J1 remains the DataFusion-only adapter/decision layer. Research: `.10x/research/2026-07-12-datafusion-pruning-evidence-readiness-audit.md`.
