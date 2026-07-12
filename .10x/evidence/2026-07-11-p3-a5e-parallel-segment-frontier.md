Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a5e-streaming-graph-integration.md, .10x/tickets/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/specs/deterministic-parallel-scheduler.md

# Parallel segment encode and canonical frontier

## What was observed

Canonical segment microbatches now enter structured CPU tasks through the injected execution host. Each task owns its Arrow input leases and conservative concat scratch reservation, encodes/hashes and durably publishes a uniquely named IPC segment, and returns an opaque package receipt. A bounded main-thread reorder map registers only the next submission ordinal, then emits the durable hook used by staged destinations and updates profile, lineage, positions, and segment evidence.

Concurrency joins effective host CPU slots, managed memory budget divided by a conservative three-times maximum-segment working set, and an evidence-selected ceiling of four. One CPU slot remains available to the destination/blocking lane. The ceiling is not identity-bearing.

## Procedure

- Inline and managed-host runs over a forced multi-segment plan produced identical manifest identity, segments, lineage, and positions and released all managed memory.
- A durable-hook failure with multiple encode tasks in flight returned the named error, cancelled/joined the task scope, retained package status `extracting`, skipped stream finalization, and released all managed memory.
- All 87 active engine tests passed before the final failure-law addition; four performance/stress tests remained ignored by policy. Focused inline/parallel identity, source-rechunk identity, ordinary staged final binding, staged failure, and generic replay tests passed.
- Strict all-target/all-feature Clippy passed for engine and project.
- Four-task TLC controls measured 2.39/1.51/1.53 seconds wall, 1.63/1.62/1.65 seconds CPU. The 1.53-second median compares with 2.39/1.55/1.79 (1.79 median) for the immediately preceding serial-encode build, but wall variance is high.
- Direct package-execution telemetry is the preferred comparison: 1.177721375 seconds versus 1.211680333 seconds, a 2.8% reduction.
- Allowing the host-wide CPU/memory join to reach nine concurrent segment tasks measured 2.37/1.81/1.55 seconds (1.81 median) and package execution 1.193407375 seconds. That configuration was rejected because encode/fsync contention erased useful concurrency.

## What this supports or challenges

This proves a production structured-concurrency frontier whose scheduling cannot change package or destination evidence. It overlaps segment work with upstream processing and improves the measured package interval. It also challenges the assumption that saturating every logical CPU with this particular mixed encode/fsync task is optimal; four tasks win on the measured host while leaving destination capacity.

## Limits

Per-phase encode/persist durations are additive task service time and therefore exceed wall time under concurrency; the lab still needs explicit service/wait/critical-path metrics. C2 partition/row-group concurrency, canonical limits, retries, and file-completion frontiers remain open. Encoding and filesystem publication are still one mixed task; a later split may improve device overlap.
