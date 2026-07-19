Status: open
Created: 2026-07-11
Updated: 2026-07-15
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/done/2026-07-11-p0-wx1-portable-partition-task-protocol.md, .10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md, .10x/tickets/2026-07-11-p3-a8-drain-epoch-executor.md

# P3 C5: isolated worker serialization equivalence law

## Scope

Build the local isolated-worker harness, route bounded and drain partition tasks through canonical serialize/drop/reconstruct/execute/result-verify/coordinator-assemble flow, and prove equivalence to direct local execution without remote transport.

## Acceptance criteria

- Harness reconstructs without borrowed resource/store/path/secret/runtime objects.
- Direct and capsule paths produce identical fixed-input segment/package/verdict/state semantics at jobs 1/N.
- Tampered/stale/missing-capability tasks/results fail before authority advances.
- High-cardinality task/result metadata remains bounded and data bypasses control messages.
- A mock source/format addition passes with registry changes only.

## Evidence expectations

Golden task/results, direct/capsule hashes, tamper/fence/generation/cancel/memory matrices, dependency scans, and adversarial distribution-seam review.

## Explicit exclusions

No RPC, remote state/artifact deployment, worker daemon, or Spark/Flink/Ballista integration.

## Blockers

Blocked on WX1 and A8; the local jobs-invariance/scaling closeout is complete.

## References

- `.10x/specs/portable-partition-task-protocol.md`
