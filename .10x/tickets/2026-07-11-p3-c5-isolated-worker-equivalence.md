Status: active
Created: 2026-07-11
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/done/2026-07-11-p0-wx1-portable-partition-task-protocol.md, .10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md, .10x/tickets/done/2026-07-11-p3-a8-drain-epoch-executor.md

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

None. WX1, C2, C4, and A8 are done; the local jobs-invariance/scaling closeout is complete.

## References

- `.10x/specs/portable-partition-task-protocol.md`

## Journal

- 2026-07-19: Activated after WS-A and its A9 conformance tail closed. Workspace-wide `cargo fmt --all -- --check`, strict all-target workspace Clippy, and strict all-features workspace Clippy pass at commit `1201792e`. The concurrent Iceberg/dependency lane remains dirty and is explicitly outside this ticket's edit and staging boundary.
- 2026-07-19: Added the neutral local isolated-host round trip in `cdf-runtime`. The host accepts only bounded serialized task/attempt bytes, reconstructs one owned source/partition/execution authority through its own registry and verifier, invokes a worker-owned executor, and returns only serialized result bytes. The coordinator API independently decodes and admits that result against its current lease, registry, artifacts, and source facts before exposing the private `AdmittedPartitionWorkerResult`. The API executes one task at a time so callers can bound the concurrent frontier without retaining cardinality-sized metadata; bulk data remains referenced rather than entering control messages. The focused round-trip/stale-fence test, all 11 worker-protocol tests, formatting, and strict all-target runtime Clippy pass.
- 2026-07-19: Added the engine-owned capsule compiler and inverse authority decoder. One real `EnginePlan` partition now lowers through an injected artifact writer into the exact source, partition, project, schema, validation, normalization, compiled-expression, operator-graph, segmentation, extent, decode-unit, and segment-plan artifacts required by WX1; the task carries only typed references and semantic hashes. The inverse verifier reads each compiler artifact under the task's memory knob, checks bytes and provider generation, decodes the actual engine types, and rejects a set that does not form one coherent plan/source/partition graph. Store transport and paths remain outside `cdf-engine`; output data verification stays on hash-while-write/provider facts rather than forced rereads. The focused real-plan compile/reconstruct/tamper test and strict all-target engine Clippy pass.
