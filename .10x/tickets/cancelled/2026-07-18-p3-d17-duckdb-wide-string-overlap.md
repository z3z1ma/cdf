Status: cancelled
Created: 2026-07-18
Updated: 2026-07-21
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
- 2026-07-21: Traced live construction and verified-package replay through `ActiveStagedIngress`, the rendezvous staging stream, DuckDB's destination-owned session, final binding, and the stock C table function. Generic orchestration already delivers each durable segment while package production continues; DuckDB currently acknowledges and retains every path, then starts its transaction and scanner only after final package verification. Per-segment scanning is rejected because the scanner parallelizes by file and would collapse to one worker. The executable candidate is a single transaction with destination-owned waves derived from admitted DuckDB native parallelism, using the same resettable finite scanner. Verified-package replay must remain deferred so duplicate receipts retain the current no-payload fast path; the staged request therefore needs a typed pre-finalization-versus-verified input phase rather than a DuckDB or path-id branch in generic orchestration.
- 2026-07-21: Falsified and deleted that candidate. It added 619 lines across the destination,
  generic staging request, project replay, and cross-destination tests, including a new generic input
  phase, a second DuckDB blocking lane, resettable scanner state, width heuristics, and a transaction
  retained across waves. The controlled EC2 TLC cell regressed from the retained 10.477-second
  median to 29.445 seconds. The candidate therefore failed its own 3% non-regression gate by a wide
  margin before the FineWeb acceptance cell could justify any of its complexity.
- 2026-07-21: The first clean-scanner EC2 run then failed before destination work because a valid
  240,920,160-byte v3 segment exceeded DuckDB's stale 128 MiB staged-byte capability. This was not
  a reason to tune generic segmentation around DuckDB. DuckDB now advertises the generic 256 MiB
  default segment envelope, with `CDF_DUCKDB_MAX_IN_FLIGHT_BYTES` as an explicit override; the
  shared memory ledger remains the actual admission authority.

## Blockers

None.

## Evidence

- `.10x/evidence/.storage/2026-07-21-p3-d17-ec2-tlc-overlap.json`: three controlled
  `c7i.4xlarge` samples at the candidate revision completed all 41,169,720 rows with a 29.445-second
  median, 1,398,206 rows/s, 2,985,099,264-byte peak RSS, zero spill, and approximately 25.116
  seconds of destination ingress. This is a rejection result, not promotion evidence.
- After cancellation, `rg` finds no `StagedInputPhase`, DuckDB overlap knob/lane, resettable scanner,
  or wave-ingress residue in production or tests.
- All 35 DuckDB tests and strict crate-wide clippy pass after the capability correction. The
  capability test requires the default DuckDB byte envelope to admit the generic default maximum
  segment, preventing this exact cross-policy mismatch from returning.

## Review

Pass for cancellation. The candidate is entirely absent from the surviving code. The sole DuckDB
product ingress remains the finite canonical package scanner, verified-package replay keeps its
duplicate-receipt no-payload fast path, and generic staging has no destination-motivated phase bit.
The existing FineWeb residual is accepted as a measured no-action: revisit only if a simpler
scanner primitive can prove a gain without adding a second ingress path or generic lifecycle state.

## Retrospective

Overlap is not automatically parallelism. Dividing a finite scanner into sequential waves reduced
the number of files available to each DuckDB scan and repeated fixed query work, turning an
architecturally elaborate candidate into a 2.8x regression. A destination optimization must first
beat the retained whole-path cell; correctness tests cannot justify keeping machinery that fails
the performance premise for which it exists.
