Status: active
Created: 2026-07-12
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Depends-On: .10x/specs/datafusion-currency-bridges.md, .10x/tickets/done/2026-07-12-p3-j0-typed-statistics-evidence-spine.md

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

None. J0 is closed with typed, manifest-bound segment/package evidence and explicit conservative absence for disabled profiles or unavailable file-grain facts.

## Progress and notes

- 2026-07-12: Readiness audit corrected the initial premise that per-column/per-segment typed evidence already existed. J0 now owns the missing neutral evidence spine; J1 remains the DataFusion-only adapter/decision layer. Research: `.10x/research/2026-07-12-datafusion-pruning-evidence-readiness-audit.md`.
- 2026-07-18: Folded in G4's performance-first profile policy. J1 may consume `stats/profile.parquet` only when the profile was explicitly emitted and verified; it must not require profile emission on ordinary hot-path runs, and it must serialize conservative retain decisions when profile evidence is disabled or absent.
- 2026-07-18: J0 closed after exact scalar-vocabulary coverage, the slim kernel envelope, and paired large-file profile-on/profile-off RSS/overhead evidence. J1 is unblocked. Its file adapter must conservatively retain when no sound file-grain typed facts exist; it may associate segment facts with a file only when existing package/lineage evidence proves that mapping.
- 2026-07-18: Activated J1 at the neutral adapter boundary. The first slice will lower an already-recorded CDF predicate into DataFusion's pruning predicate, marshal complete J0 typed bounds into vectorized `PruningStatistics`, return conservative unknowns for every incomplete/unsupported fact, and expose decisions without adding DataFusion types below `cdf-engine`. Consumer-specific replay/backfill/SQL/merge integration remains subsequent work in this ticket.
- 2026-07-18: Implemented the bounded neutral adapter slice. `cdf-package` now exposes sealed whole-container windows only after a complete first-pass verification of the profile and rereads the same verified identity object in caller-sized windows; provisional visitor rows cannot become skip authority. `cdf-engine` binds pruning to an indexed predicate in the digest-verified `CompiledExpressionPlan`, performs schema-directed literal lowering without reoptimization, supplies DataFusion only complete typed dimensions, and serializes DataFusion-free decisions. Unsupported decimal/timezone predicates and incomplete/NaN facts retain conservatively. The double profile read is deliberate bounded verification, not a payload read; consumer integration must still install shared-memory admission and record skipped byte/unit evidence.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-package verified_statistics_profile_is_manifest_bound_typed_parquet --lib --locked -j 12` — passed 1/1; proves complete-profile verification precedes sealed, caller-sized whole-container windows and rejects a zero window knob.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine statistics_pruning --lib --locked -j 12` — passed 4/4; covers exact Int32 typed lowering, impossible-versus-may-match decisions, incomplete/NaN/all-null null soundness, conservative decimal/timezone handling, stale schema/type/shape rejection, and compiled-plan digest tampering.
- Strict all-target Clippy passed for `cdf-package` and `cdf-engine`. The exact active fast-CI Rust surface passed: locked metadata, workspace formatting, core library Clippy, 383 kernel/contract/package/runtime tests with eight performance tests ignored, 35 CLI-core tests, 37 CLI-artifact tests, and strict all-feature CLI-core Clippy. Gitleaks 8.18.4 found no leak in the exact staged diff.
- A diagnostic full `cdf-engine` library run remains at the committed-main non-fast baseline of seven unrelated fixture/ownership failures (invalid historical file hashes/accounting, batch-rechunking identity, widening expectation, and benchmark-owned thread scans). This slice adds four passing tests and does not alter those owners; the active fast-CI decision intentionally excludes the full engine fixture surface. The prior baseline and ownership are recorded in `.10x/tickets/done/2026-07-11-p3-c5-isolated-worker-equivalence.md`.

## Review

- The independent bounded-slice review initially failed with three significant findings: raw provisional evidence and a detached predicate were public; generic re-lowering lost narrower physical literal types; incomplete evidence still exposed a null count that could authorize an `is_null` skip.
- All three were repaired before commit. The public boundary now requires the sealed verified package window and the complete compiled plan; typed lowering is schema-directed and Int32 is exercised through the product adapter; unsupported types retain; and every statistic dimension, including null count, becomes unknown when row completeness is unavailable. DataFusion remains confined to `cdf-engine`.
- Residual risk remains intentionally open under this ticket: consumers are not yet wired, window memory is not yet leased from `cdf-memory`, decisions are not yet streamed/spilled, and the expression IR does not yet encode decimal/temporal literals capable of selective pruning. Those types are soundly retained today rather than falsely advertised as optimized.
