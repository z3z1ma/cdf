Status: open
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-18-p3-d14-duckdb-nanoarrow-080-lz4-revalidation.md

# P3 D17: restore wide-string DuckDB overlap on the sole canonical scanner

## Scope

Restore the lost package/destination overlap for wide, long-string Arrow payloads without reviving the appender, nanoarrow, callback stream, duplicate handoff, or any second DuckDB product ingress path. Measure the current canonical scanner against an exact wide-string raw reference, then make its lifecycle progressively consume durable canonical segments through the same scanner primitive if and only if controlled evidence preserves or improves both the wide-string and TLC envelopes.

## Non-goals

- No destination identity in generic orchestration.
- No appender, nanoarrow/custom DuckDB build, Parquet handoff, callback C stream, scalar path, compatibility shim, or runtime fallback.
- No hard-coded thread, memory, segment, or byte cap; all useful limits derive from recorded execution authority or explicit knobs.
- No throughput claim from public-network or one-off laptop timing.
- No weakening package finalization, transaction rollback, receipt verification, compact provenance, replay, or checkpoint ordering.

## Acceptance Criteria

- A controlled wide-string reference isolates canonical IPC decode plus DuckDB materialization for the exact 2,147,509,487-byte FineWeb object/package shape and records wall, CPU, RSS/cgroup peak, logical bytes, rows, and scanner concurrency.
- The sole `canonical_segment_scan` product path overlaps durable segment consumption with package production through destination-owned lifecycle code; generic runtime capabilities remain the only orchestration input.
- Same-host, median-of-N FineWeb local wall returns to within 10% of the latest pre-stock-scanner governed observation, or the ticket records a lower measured hardware/library roofline and is cancelled without retaining slower machinery.
- The controlled full-year TLC median does not regress by more than 3% from the current `10.477064330s` median, and the ordinary lab-wide 10% regression gate remains green.
- Default process RSS stays within the resolved process authority on a calibrated host; a 6 GiB enforced EC2 cell completes with zero pressure/OOM/spill unless spill is explicitly part of the tested policy.
- Append, replace, merge, replay, duplicate receipt, failure/abort, row-provenance, and jobs-invariance conformance pass through the one surviving scanner implementation.
- All superseded experiments and dead code are absent from `cdf-dest-duckdb` after closure.

## References

- `.10x/evidence/2026-07-18-final-correctness-performance-pass.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`
- `.10x/decisions/destination-ingress-protocol-capability-split.md`
- `.10x/tickets/done/2026-07-18-p3-d14-duckdb-nanoarrow-080-lz4-revalidation.md`

## Assumptions

- Record-backed: the stock canonical scanner is the sole DuckDB product path and the controlled full-year TLC median is healthy at `10.477064330s` on the named EC2 host.
- Record-backed: current local FineWeb package execution and final DuckDB binding are serialized at approximately `5.974s + 7.377s`; explicit thread reduction does not recover the gap, while smaller native memory limits materially reduce throughput.
- User-ratified: performance must not be traded away for convenience; useful limits are knobs/authority-derived rather than hard-coded; no legacy or second happy path may remain.

## Journal

- 2026-07-18: Opened from the final current-tree audit. The public-network path is healthy relative to its paired raw transfer and controlled TLC remains within 2.16% of its retained median, so this is deliberately scoped to wide-string destination overlap rather than reopened as a generic remote-I/O or runtime regression.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
