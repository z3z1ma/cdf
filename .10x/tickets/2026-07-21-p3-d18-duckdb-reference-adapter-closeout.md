Status: open
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-18-p3-d14-duckdb-nanoarrow-080-lz4-revalidation.md, .10x/tickets/done/2026-07-21-p0-duckdb-wide-ingest-memory.md

# P3 D18: DuckDB reference-adapter closeout

## Scope

Close the remaining measured performance, memory-authority, observability, and avoidable type-fidelity
gaps in CDF's sole stock-libduckdb canonical-segment ingress. Preserve the existing destination-owned
public-C-API table function and delete every experiment that does not improve a controlled workload.

This parent is an aggregate plan and is not executable. Its children are:

1. `.10x/tickets/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md`
2. `.10x/tickets/2026-07-21-p3-d18b-duckdb-sparse-wide-projection.md`
3. `.10x/tickets/2026-07-21-p3-d18c-duckdb-native-write-envelope.md`
4. `.10x/tickets/2026-07-21-p3-d18d-duckdb-physical-admission-telemetry.md`
5. `.10x/tickets/2026-07-21-p3-d18e-duckdb-public-abi-scanner-envelope.md`
6. `.10x/tickets/2026-07-21-p3-d18f-duckdb-lossless-type-closure.md`
7. `.10x/tickets/2026-07-21-p3-d18z-duckdb-reference-adapter-gate.md`

## Non-goals

- No second product ingress, appender, nanoarrow/custom DuckDB runtime, Parquet handoff, deprecated
  Arrow stream scanner, destination identity in generic orchestration, or compatibility shim.
- No performance default without same-host before/after evidence.
- No weakening package identity, compact provenance, transaction/receipt/checkpoint semantics, or
  exact type fidelity.
- No hiding an impossible exact mapping behind an implicit lossy cast.

## Acceptance Criteria

- The exact 2,052-column sparse-wide package has a raw DuckDB/CDF scanner profile and comparable
  roofline, not only a completion timing.
- Complete package statistics can eliminate provably all-null payload work without removing target
  columns or changing visible values, and the retained implementation improves the wide cell without
  regressing TLC.
- DuckDB-native write-buffer and database row-group policies are benchmarked; only a schema/host-
  derived or explicit-knob policy that improves the governed envelope survives.
- Scan admission uses physical package facts where available, preserves a conservative first-plan
  path, accounts for non-buffer-manager headroom, and records effective settings/retries/peak/spill
  evidence without entering package identity.
- The stock public-C-API scanner is profiled against its retained raw comparator and any retained
  optimization preserves its single-path, bounded, exact-ownership design.
- Every avoidable lossless Arrow mapping is admitted automatically; fidelity-impossible mappings
  remain precise plan-time failures with both types and semantic fixes.
- The final controlled EC2 matrix proves no more than 3% regression for full-year TLC, a material
  improvement or measured DuckDB-native floor for the wide cell, bounded memory, and exact
  append/replace/merge/replay/duplicate/correction/provenance semantics.
- All rejected prototypes and superseded code are absent at closure.

## References

- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/runtime-memory-backpressure.md`
- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/specs/typed-statistics-evidence.md`
- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`
- `.10x/decisions/terabyte-scale-performance-envelope.md`
- `.10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`

## Assumptions

- User-ratified 2026-07-21: execute every remaining DuckDB improvement identified by the reference-
  adapter audit, commit and push incrementally, and do not retain regressions or alternate paths.
- Record-backed: the stock public-C-API scanner is the sole product ingress and its full-year TLC
  median is `10.255642670s` for 41,169,720 rows on the controlled EC2 host.
- Record-backed: the exact default wide replay completes in `98.66s`, peaks at 5,121,736,704 RSS
  bytes, and spills under a 4 GiB DuckDB buffer-manager budget; that proves robustness, not a
  performance roofline or process-memory ceiling.

## Journal

- 2026-07-21: Opened after the user rejected treating the repaired wide-schema survivor as the
  endpoint. Work is split so measurement precedes tuning and each independent candidate can be
  deleted if it fails its own performance premise.

## Blockers

None at parent level. Child dependencies govern execution.

## Evidence

Pending child closure.

## Review

Pending child closure and final independent adversarial review.

## Retrospective

Pending.
