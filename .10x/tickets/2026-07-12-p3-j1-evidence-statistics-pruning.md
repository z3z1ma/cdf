Status: open
Created: 2026-07-12
Updated: 2026-07-18
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
- Disabled or absent `stats/profile.parquet` is treated exactly like missing evidence: pruning retains the affected unit and records the conservative fallback.

## Evidence expectations

Property/differential tests, corrupt/stale evidence adversaries, skipped-byte benchmarks, replay/backfill/sql/merge fixtures, dependency checks, and review.

## Explicit exclusions

No new statistics artifact schema unless separately ratified; no payload rewrite or package identity change.

## Blockers

J0 must provide sound typed, manifest-bound evidence for the grains J1 wants to prune. The 2026-07-12 readiness audit found lexical `BatchStats` unpopulated and `profile.json` aggregate-only; adapting either would be unsound or useless. As of 2026-07-18, profile-enabled segment/package `stats/profile.parquet` evidence exists, but J1 still depends on J0's explicit pruning-readiness limits: missing/disabled profile evidence must conservatively retain, and any required file/source grains, type admissions, RSS, and overhead gates remain J0-owned until closed.

## Progress and notes

- 2026-07-12: Readiness audit corrected the initial premise that per-column/per-segment typed evidence already existed. J0 now owns the missing neutral evidence spine; J1 remains the DataFusion-only adapter/decision layer. Research: `.10x/research/2026-07-12-datafusion-pruning-evidence-readiness-audit.md`.
- 2026-07-18: Folded in G4's performance-first profile policy. J1 may consume `stats/profile.parquet` only when the profile was explicitly emitted and verified; it must not require profile emission on ordinary hot-path runs, and it must serialize conservative retain decisions when profile evidence is disabled or absent.
