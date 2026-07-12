Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md, .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md

# P3 C2: parallel partition/unit execution and canonical frontier

## Scope

Execute admitted partitions/format units concurrently through the streaming graph, implement bounded reorder/frontier, canonical limit/slicing/speculative discard, partition retry/reattest, file completion, and scope-safe state assembly.

## Acceptance criteria

- Random completion/delay/retry/jobs produce jobs=1-identical packages/evidence/state.
- Reorder memory remains bounded with a stalled first partition; later admission backpressures.
- Limits at every row/batch/unit/partition boundary select the exact jobs=1 prefix at the compiled limit node and never advance discarded positions.
- Retried identity/schema changes fail/replan; duplicate attempts cannot cross the frontier.
- File positions advance only after every selected row-group/block/member unit completes.

## Evidence expectations

Randomized/property scheduler harness, jobs goldens, limit/position matrix, retry/identity chaos, slow-frontier memory traces, cancellation/leak tests, and scaling profiles.

## Explicit exclusions

No DataFusion/Python-specific tuning or distributed work.

## Blockers

Depends on C1, A5, and A3.

## References

- `.10x/specs/deterministic-parallel-scheduler.md`
