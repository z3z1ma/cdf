Status: open
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/tickets/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md, .10x/tickets/2026-07-07-local-partition-parallelism-triage.md

# P3 WS-C: parallelism with deterministic assembly

## Scope

Execute logical file, row-group, window, and other safe partitions concurrently under `--jobs` and the memory ledger. Fix partition-to-segment assignment at plan time, preserve source rate/scope constraints, serialize single-writer destinations where required, and make output hashes invariant to scheduling.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md`
- `.10x/tickets/2026-07-11-p3-c2-parallel-frontier-execution.md`
- `.10x/tickets/2026-07-11-p3-c3-engine-ffi-parallel-integration.md`
- `.10x/tickets/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md`
- `.10x/tickets/2026-07-11-p3-c5-isolated-worker-equivalence.md`

## Acceptance criteria

- `--jobs 1` and `--jobs N` produce identical manifest hashes for every permanent fixture.
- Cancellation, first-error propagation, retry units, source positions, and checkpoint scopes remain deterministic.
- Scaling is measured until the relevant device/destination saturates.
- Python behavior is equivalent on GIL and free-threaded interpreters, with only concurrency differing.
- Direct-local and canonical serialized isolated-worker execution are byte/semantics equivalent for fixed partition plans, preserving the future distribution seam without shipping a remote scheduler.

## Blockers

C1 admission, the injected execution host, memory ledger, and WS-L baseline are complete. C2 production frontier execution is next; SX1's compiler/discovery hooks and A3 closure evidence continue independently where their active tickets specify.

## References

- `.10x/decisions/canonical-frontier-parallel-scheduling.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
