Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md, .10x/tickets/2026-07-07-local-partition-parallelism-triage.md

# P3 WS-C: parallelism with deterministic assembly

## Scope

Execute logical file, row-group, window, and other safe partitions concurrently under `--jobs` and the memory ledger. Fix partition-to-segment assignment at plan time, preserve source rate/scope constraints, serialize single-writer destinations where required, and make output hashes invariant to scheduling.

## Acceptance criteria

- `--jobs 1` and `--jobs N` produce identical manifest hashes for every permanent fixture.
- Cancellation, first-error propagation, retry units, source positions, and checkpoint scopes remain deterministic.
- Scaling is measured until the relevant device/destination saturates.
- Python behavior is equivalent on GIL and free-threaded interpreters, with only concurrency differing.

## Blockers

Blocked on WS-A channels and ledger plus WS-L baseline.
