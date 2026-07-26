Status: done
Created: 2026-07-12
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/specs/datafusion-currency-bridges.md, .10x/tickets/done/2026-07-12-p3-j0-typed-statistics-evidence-spine.md

# P3 J1: evidence statistics pruning

## Scope

Implement a destination-neutral, streaming verified-package segment-selection authority over CDF
segment/package profile evidence. It compiles one predicate from the recorded
`CompiledExpressionPlan` into sound DataFusion pruning decisions without opening skipped
payloads, exposes only retained verified-segment capabilities, and records an auditable summary
for future replay, backfill, package-query, and merge consumers.

The 2026-07-25 stabilization grooming deliberately does not activate new identity-bearing command
semantics merely to demonstrate the adapter. The concrete consumer delivered here is the neutral
verified-package planner those commands can reuse after their own semantics are ratified.

## Acceptance criteria

- Missing/incompatible/stale statistics conservatively retain data.
- NULL, NaN, decimal, timezone, cast, nested, schema-evolution, and absent-stat cases are sound.
- The concrete verified-package consumer is differentially equivalent to an unpruned segment scan
  for supported predicates and never opens skipped segment payloads.
- Planner records predicate, evidence generation, skipped units/bytes, and conservative fallbacks.
- Pruning code lives in an engine adapter; package/stat artifacts expose no DataFusion types.
- Disabled or absent `stats/profile.parquet` is treated exactly like missing evidence: pruning retains the affected unit and records the conservative fallback.
- Profile and transient pruning memory share one caller-sized `cdf-memory` reservation; decisions
  stream without a package-cardinality collection.

## Evidence expectations

Property/differential tests, corrupt/stale evidence adversaries, skipped-byte assertions,
dependency checks, and review.

## Explicit exclusions

No new statistics artifact schema, payload rewrite, package identity change, or activation of new
`replay --where`, backfill, package-SQL, or merge semantics. File-grain skipping remains
conservative absence because the current artifact intentionally contains only segment and package
grains.

## Blockers

None. J0 is closed with typed, manifest-bound segment/package evidence and explicit conservative absence for disabled profiles or unavailable file-grain facts.

## Progress and notes

- 2026-07-12: Readiness audit corrected the initial premise that per-column/per-segment typed evidence already existed. J0 now owns the missing neutral evidence spine; J1 remains the DataFusion-only adapter/decision layer. Research: `.10x/research/2026-07-12-datafusion-pruning-evidence-readiness-audit.md`.
- 2026-07-18: Folded in G4's performance-first profile policy. J1 may consume `stats/profile.parquet` only when the profile was explicitly emitted and verified; it must not require profile emission on ordinary hot-path runs, and it must serialize conservative retain decisions when profile evidence is disabled or absent.
- 2026-07-18: J0 closed after exact scalar-vocabulary coverage, the slim kernel envelope, and paired large-file profile-on/profile-off RSS/overhead evidence. J1 is unblocked. Its file adapter must conservatively retain when no sound file-grain typed facts exist; it may associate segment facts with a file only when existing package/lineage evidence proves that mapping.
- 2026-07-18: Activated J1 at the neutral adapter boundary. The first slice will lower an already-recorded CDF predicate into DataFusion's pruning predicate, marshal complete J0 typed bounds into vectorized `PruningStatistics`, return conservative unknowns for every incomplete/unsupported fact, and expose decisions without adding DataFusion types below `cdf-engine`. Consumer-specific replay/backfill/SQL/merge integration remains subsequent work in this ticket.
- 2026-07-18: Implemented the bounded neutral adapter slice. `cdf-package` now exposes sealed whole-container windows only after a complete first-pass verification of the profile and rereads the same verified identity object in caller-sized windows; provisional visitor rows cannot become skip authority. `cdf-engine` binds pruning to an indexed predicate in the digest-verified `CompiledExpressionPlan`, performs schema-directed literal lowering without reoptimization, supplies DataFusion only complete typed dimensions, and serializes DataFusion-free decisions. Unsupported decimal/timezone predicates and incomplete/NaN facts retain conservatively. The double profile read is deliberate bounded verification, not a payload read; consumer integration must still install shared-memory admission and record skipped byte/unit evidence.
- 2026-07-25: Backlog grooming retained J1 as the sole active DataFusion currency bridge and
  reparented it directly to P3. Broader object-store/catalog/plan-marshaling adoption is parked;
  this ticket remains justified by a concrete existing evidence spine and bounded consumer value.
- 2026-07-25: Completed the concrete consumer as
  `for_each_verified_package_segment_pruning`. It verifies the package/profile before making any
  skip decision, aligns each decision to canonical manifest ordinal/id/row count, exposes a
  `VerifiedSegmentObject` only for retained segments, and records profile presence, package
  generation, recorded predicate, skipped/retained rows and bytes, and conservative counts.
  Missing profile evidence streams every segment as conservative retention.
- 2026-07-25: Added one shared-memory admission boundary to package profile windows. The caller
  supplies both container and byte knobs; `cdf-package` holds one package-class lease across the
  borrowed window callback, and `cdf-engine` proves its conservative evidence-plus-Arrow
  working-set model fits that reservation before evaluating the predicate. Decisions and segment
  capabilities stream to the caller, so planner memory is independent of package cardinality.
- 2026-07-25: Scope review rejected wiring an invented `replay --where` command solely to satisfy
  the original illustrative consumer list. Filtering replay would be a new identity/commit
  semantic, not a neutral proof. The stabilization program had already narrowed J1 to one concrete
  consumer path; the verified-package segment planner is that path and preserves the active
  DataFusion identity boundary.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-package verified_statistics_profile_is_manifest_bound_typed_parquet --lib --locked -j 12` — passed 1/1; proves complete-profile verification precedes sealed, caller-sized whole-container windows and rejects a zero window knob.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine statistics_pruning --lib --locked -j 12` — passed 4/4; covers exact Int32 typed lowering, impossible-versus-may-match decisions, incomplete/NaN/all-null null soundness, conservative decimal/timezone handling, stale schema/type/shape rejection, and compiled-plan digest tampering.
- Strict all-target Clippy passed for `cdf-package` and `cdf-engine`. The exact active fast-CI Rust surface passed: locked metadata, workspace formatting, core library Clippy, 383 kernel/contract/package/runtime tests with eight performance tests ignored, 35 CLI-core tests, 37 CLI-artifact tests, and strict all-feature CLI-core Clippy. Gitleaks 8.18.4 found no leak in the exact staged diff.
- A diagnostic full `cdf-engine` library run remains at the committed-main non-fast baseline of seven unrelated fixture/ownership failures (invalid historical file hashes/accounting, batch-rechunking identity, widening expectation, and benchmark-owned thread scans). This slice adds four passing tests and does not alter those owners; the active fast-CI decision intentionally excludes the full engine fixture surface. The prior baseline and ownership are recorded in `.10x/tickets/done/2026-07-11-p3-c5-isolated-worker-equivalence.md`.
- `CARGO_BUILD_JOBS=12 cargo nextest run -p cdf-package -p cdf-engine --no-fail-fast` — passed
  280/280 with 11 explicitly skipped. This supersedes the old diagnostic baseline above on current
  HEAD and proves the package verification, engine, memory, and pruning suites integrate.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-package -p cdf-engine --all-targets --locked -j 12 -- -D warnings`
  — passed.
- `verified_package_consumer_skips_payloads_and_matches_unpruned_rows` builds and verifies a
  two-segment canonical package, proves `id >= 5` skips the impossible segment without opening it,
  opens only the retained verified capability, and obtains the same selected rows as the unpruned
  scan. The summary records one skipped and one retained segment with nonzero byte counts.
- `missing_profile_streams_every_segment_as_conservative_retention` proves profile absence cannot
  authorize a skip. `integer_pruning_never_skips_a_matching_value` checks 2,000 deterministic
  integer range/predicate combinations and asserts every pruned range contains no matching value.
- `verified_statistics_profile_is_manifest_bound_typed_parquet` now proves the package window
  retains a single 256 KiB managed reservation across callbacks, releases it to zero afterward,
  and rejects invalid zero container/byte limits.

## Review

- The independent bounded-slice review initially failed with three significant findings: raw provisional evidence and a detached predicate were public; generic re-lowering lost narrower physical literal types; incomplete evidence still exposed a null count that could authorize an `is_null` skip.
- All three were repaired before commit. The public boundary now requires the sealed verified package window and the complete compiled plan; typed lowering is schema-directed and Int32 is exercised through the product adapter; unsupported types retain; and every statistic dimension, including null count, becomes unknown when row completeness is unavailable. DataFusion remains confined to `cdf-engine`.
- The bounded-slice review had left consumer wiring, shared-memory leasing, and streamed decisions
  open; the final slice closes those three findings. The expression IR still cannot encode every
  decimal/temporal literal combination, so those types are soundly retained rather than falsely
  advertised as optimized.
- Final fresh-hat control-flow review traced manifest verification, complete profile verification,
  second-pass bounded windows, DataFusion lowering, manifest-decision alignment, capability
  exposure, and caller segment reads. The former memory/streaming/consumer residuals are closed:
  one shared reservation covers each borrowed window, decisions stream, and a verified-package
  consumer opens retained capabilities only.
- Verdict: pass. The sole retained limitation is deliberate and sound: unsupported
  decimal/temporal/nested/cast predicates and absent file-grain facts retain conservatively.
  Activating command-specific replay/backfill/query/merge semantics remains future scoped work,
  not a J1 correctness gap.

## Retrospective

- The useful boundary was smaller than the original list of downstream features. One neutral
  verified-segment selection authority gives every future consumer the hard, reusable part without
  laundering new command semantics into a performance ticket.
- Sealed evidence is insufficient unless memory ownership is sealed with it. Borrowing the window
  only inside a callback makes the lease lifetime structural and prevents an engine adapter from
  retaining unaccounted profile rows.
- Skipping safety needs two independent proofs: statistical soundness and authority alignment.
  DataFusion proves the former; explicit ordinal/id/row-count joins against the verified manifest
  prove the latter.
- The earlier full-engine failures were not durable truth. Re-running the current integrated
  surface produced 280/280 green tests, so terminal records must distinguish historical evidence
  from current evidence rather than perpetuate stale baselines.
