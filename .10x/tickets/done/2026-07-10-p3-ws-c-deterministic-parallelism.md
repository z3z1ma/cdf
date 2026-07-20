Status: done
Created: 2026-07-10
Updated: 2026-07-20
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md, .10x/tickets/done/2026-07-07-local-partition-parallelism-triage.md

# P3 WS-C: parallelism with deterministic assembly

## Scope

Execute logical file, row-group, window, and other safe partitions concurrently under `--jobs` and the memory ledger. Fix partition-to-segment assignment at plan time, preserve source rate/scope constraints, serialize single-writer destinations where required, and make output hashes invariant to scheduling.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md`
- `.10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md`
- `.10x/tickets/done/2026-07-11-p3-c3-engine-ffi-parallel-integration.md`
- `.10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md`
- `.10x/tickets/done/2026-07-11-p3-c5-isolated-worker-equivalence.md`

## Acceptance criteria

- `--jobs 1` and `--jobs N` produce identical manifest hashes for every permanent fixture.
- Cancellation, first-error propagation, retry units, source positions, and checkpoint scopes remain deterministic.
- Scaling is measured until the relevant device/destination saturates.
- Python behavior is equivalent on GIL and free-threaded interpreters, with only concurrency differing.
- Direct-local and canonical serialized isolated-worker execution are byte/semantics equivalent for fixed partition plans, preserving the future distribution seam without shipping a remote scheduler.

## Blockers

None. C1–C5 and every prerequisite are complete.

## Progress and notes

- 2026-07-14: Closed C2 after the source-neutral canonical frontier passed a fresh significant-only review and 537 affected-graph tests. The active-ticket directory fell from 85 to 84 records (15 active, 63 open, 6 blocked), with 299 tickets now done. C3 is activated next because it is the only remaining dependency before C4 can exercise the system-wide jobs-invariance/scaling matrix.
- 2026-07-14: Closed C3 after 247 affected-graph tests and fresh adversarial pass. Codec, DataFusion, Python, and native/FFI work now share bounded CPU authority with post-release completion, global mixed-cost fairness, and worker-safe teardown. The active-ticket directory fell from 84 to 83 records and done tickets rose from 299 to 300. C4 is activated because it has the highest immediate dependency fanout in P3.
- 2026-07-15: Fresh C4 review rejected closure. The concrete re-entrant memory-waker hazard is fixed in `b4e7ec6d`; C4 remains blocked on D8 and its own missing exact matrix/telemetry evidence. D8 now depends only on the completed C3 substrate, eliminating the prior dependency cycle.
- 2026-07-15: D8 closed with full-path FineWeb throughput above its required reference ratio and exact logical destination receipt/manifest identity at jobs 1/2/auto/4. C4 is reactivated as the highest-fanout P3 closure ticket.
- 2026-07-15: Closed C4 after a fresh staged-path FineWeb jobs 1/2/4 curve named the current jobs=2 knee, generic task/permit/frontier telemetry made scheduler overhead and speculative waste observable without adapter leaks, and complete logical receipts remained invariant across DuckDB, Parquet, PostgreSQL destination, REST, and SQL-source matrices. C5 now waits only on the portable task protocol and drain-epoch executor.
- 2026-07-20: Closed C5 after direct and serialized isolated-worker execution produced identical canonical segments, packages, verdicts, source positions, finite-drain evidence, and terminal schema-quarantine semantics at jobs 1/N. Task/result authority is bounded, registry-driven, fenced at mutation time, and sealed to exact compiled plan identity. All five children are terminal; WS-C is complete.

## References

- `.10x/decisions/canonical-frontier-parallel-scheduling.md`
- `.10x/specs/deterministic-parallel-scheduler.md`

## Evidence

- C1 establishes hierarchical scheduler admission, deterministic cancellation, retry, scope, and memory authority.
- C2 establishes canonical frontier execution and deterministic first-error/partition ordering under parallel polling.
- C3 joins DataFusion, native/FFI, and Python lanes to shared CPU authority; real GIL and free-threaded Python runs produce identical work hashes with only concurrency differing.
- C4 permanently compares jobs 1/2/auto/N semantics across file formats, REST, SQL sources, DuckDB, Parquet, and PostgreSQL, and records the measured scaling knee and saturation evidence.
- C5 proves byte/semantic equivalence between direct-local and serialized isolated execution for the compiler-proven partition-separable topology, including bounded, multi-partition, finite-drain, and schema-quarantine cases.

## Review

- Every executable child passed an independent adversarial review. C5's final review explicitly attempted to falsify distribution authority, boundedness, fencing, plan replay, and result semantics and returned `pass` after one consolidated repair batch.
- Parent closure finds every acceptance criterion mapped to child evidence and no unresolved critical or significant finding.

## Retrospective

- Deterministic parallelism required separating scheduling freedom from identity authority: jobs may change timing and utilization, never canonical ordering or package semantics.
- The portable worker seam is smallest when it distributes only compiler-proven partition-separable work. Global operators need their own typed topology and must not be approximated by partition capsules.
- Capability registries, sealed admitted types, shared resource admission, and last-moment provider fencing keep source/destination identity out of generic orchestration while preserving a future distributed execution boundary.
