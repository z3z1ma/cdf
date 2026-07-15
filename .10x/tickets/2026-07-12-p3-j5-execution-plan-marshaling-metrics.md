Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Depends-On: .10x/tickets/2026-07-11-p0-wx1-portable-partition-task-protocol.md, .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md

# P3 J5: ExecutionPlan marshaling and unified metrics

## Scope

Represent CDF's native scan, validation, canonical segment, persistence, and staged-ingress operators as DataFusion `ExecutionPlan` shells; expose unified explain-analyze metrics and recorded physical properties; marshal portable plan references through WX1 task capsules for later Ballista/Spark/Flink evaluation.

## Acceptance criteria

- Native operators retain all identity semantics and golden bytes.
- DataFusion schedules/metrics the graph without receiving credential, callback, borrowed-object, or package-finalization authority.
- Explain-analyze reports one operator tree with rows, bytes, waits, spill, elapsed/CPU, and native lane metrics.
- Jobs 1/N and direct/capsule execution remain byte-identical.
- DataFusion protobuf/Ballista forms are translations around the canonical WX1 task, never sole authority.
- Physical optimizer rewrites are allowlisted, recorded, and rejected when they could change CDF identity.

## Evidence expectations

Jobs/capsule goldens, optimizer adversaries, metric-accounting tests, serialization compatibility/tamper fixtures, explain overhead benchmarks, and review.

## Explicit exclusions

No distributed scheduler, remote worker daemon, or generic DataFusion package writer.

## Blockers

WX1, A5, and C2.
