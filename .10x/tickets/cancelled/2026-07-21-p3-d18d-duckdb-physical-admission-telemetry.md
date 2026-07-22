Status: cancelled
Created: 2026-07-21
Updated: 2026-07-22
Parent: .10x/tickets/done/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md
Depends-On: .10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md

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
- The configured DuckDB temp-directory ceiling is applied to the live buffer manager after open and
  proven with a spill workload; `current_setting()` alone is not enforcement evidence. Automatic
  policy must preserve successful no-tuning wide ingestion, and an explicit operator knob remains
  authoritative.
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
- `.10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md`
- `https://duckdb.org/docs/current/operations_manual/limits`

## Assumptions

- Record-backed: DuckDB's `memory_limit` governs its buffer manager rather than total process RSS.
- Record-backed: physical tuning belongs in nonidentity run/receipt evidence.

## Journal

- 2026-07-22: D18A found an exact DuckDB 1.5.4 open-time configuration gap. CDF's writer
  connection reports `max_temp_directory_size=1.0 GiB`, while the same query's native profile
  observes a 6,973,063,168-byte peak. In tag `08e34c447b`, open-time
  `MaxTempDirectorySizeSetting::SetGlobal` stores `DBConfig::options.maximum_swap_space`, but
  `DatabaseInstance::Initialize` constructs `StandardBufferManager` without transferring that
  value to its live temporary-directory limiter. This ticket must apply the effective limiter after
  open and verify it behaviorally; it must also benchmark the automatic ceiling rather than turning
  the nominal 1 GiB value into a default failure for the 2,052-column reference workload.
- 2026-07-22: Cancelled by explicit user direction as part of the DuckDB closeout. Current
  schema-derived admission, typed OOM rollback/redrive, explicit memory/concurrency controls, and
  successful no-tuning wide completion remain the product boundary. The DuckDB 1.5.4 live
  temp-limiter mismatch and schema-width final-binding statistics allocation remain accepted,
  documented residuals rather than claims of enforced limits.

## Blockers

None. Cancellation is deliberate, not blocked.

## Evidence

D18A retains exact writer settings plus observed native buffer/temp peaks; D18B retains the final
wide/TLC cgroup evidence. No D18D implementation was attempted.

## Review

Cancellation adds no unmeasured resource policy and does not weaken the existing typed OOM safety
backstop.

## Retrospective

Configuration values reported by an embedded engine are not enforcement evidence. Reopening this
work requires a behaviorally verified live-limit API and representative performance evidence.
