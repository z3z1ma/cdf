Status: open
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md
Depends-On: .10x/tickets/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md

# P3 D18D: DuckDB physical admission and telemetry

## Scope

Replace schema-only first admission where verified physical package facts are available, reserve
explicit process headroom for Arrow/C-API allocations outside DuckDB's buffer manager, and record
the effective ingest envelope. Keep typed OOM rollback/redrive as the final correctness backstop.

## Non-goals

No package-identity dependence on host tuning, RSS as an allocation primitive, payload pre-read,
unbounded native allocation, or generic runtime knowledge of DuckDB settings.

## Acceptance Criteria

- Admission consumes verified per-segment/package physical bytes, null density, variable-width
  bytes, batch bounds, compiled schema, native vector size, and process/native headroom when present;
  absent/incomplete facts use a conservative schema-derived estimate.
- Arrow reader/conversion memory and DuckDB buffer-manager authority cannot together claim the full
  process budget; explicit memory and scan-thread knobs remain authoritative.
- Run/receipt evidence records global threads, scan threads, estimated and observed worker bytes,
  retries, DuckDB peak buffer memory, peak temp-directory size, and spill without affecting package
  identity.
- Automatic OOM retry remains transactionally clean and reports every attempt; OS/cgroup pressure
  cannot be misreported as a typed DuckDB retry.
- Wide and TLC controlled cells remain within their performance gates and process/cgroup ceilings.

## References

- `.10x/specs/runtime-memory-backpressure.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/tickets/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md`
- `https://duckdb.org/docs/current/operations_manual/limits`

## Assumptions

- Record-backed: DuckDB's `memory_limit` governs its buffer manager rather than total process RSS.
- Record-backed: physical tuning belongs in nonidentity run/receipt evidence.

## Journal

None.

## Blockers

Depends on D18A baseline/profile.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
