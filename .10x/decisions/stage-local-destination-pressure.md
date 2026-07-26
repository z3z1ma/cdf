Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Keep destination pressure local to the destination stage

## Context

CDF's scheduler currently joins a staged destination's retained-segment window into the
run-wide jobs minimum when `--jobs` is absent. For the Parquet destination, a two-segment window
therefore reduces a 16-slot host and a 16-way source to two run-wide jobs. The 1 TiB closeout run
recorded 47 process threads but only 128% average CPU over 44:55, consistent with this admission
collapse.

That interpretation contradicts the more specific runtime contracts:
`.10x/specs/deterministic-parallel-scheduler.md` says a destination serializes only its writer
lane while upstream work remains parallel until bounded backpressure joins, and
`.10x/decisions/schema-planned-destination-bulk-paths.md` says destination staging limits flow
through the common graph rather than becoming a global orchestration branch.

Parquet compounds the problem. Its physical path advertises two writers, but its live staged
session owns one active object group. Completed groups may publish while the next group encodes,
but independent groups do not perform row encoding concurrently. The previous four-writer
falsification changed the declared limit without changing that single-active-group topology, so
it is not evidence against real parallel group encoding.

## Decision

Run-wide jobs and stage-local capacity are distinct authorities.

The run-wide jobs authority MUST join only resources consumed by every concurrently active leaf:
the explicit jobs ceiling, effective host/cgroup CPU slots, source capability, source working-set
memory, source blocking lanes, transport authority, and checkpoint/scope serialization where
applicable. A destination's retained-item window, retained-byte window, prepared writer count,
or single-writer lane MUST NOT reduce the run-wide upstream jobs ceiling.

Destination pressure MUST remain enforced at the destination stage through the compiled operator
graph, item- and byte-bounded channels, the destination blocking lane, and the shared memory
ledger. Those bounds remain visible in scheduler and run evidence as stage capacities, not as
run-wide limiting factors.

Bulk-path writer concurrency MUST be prepared from the execution authority available to that
attempt: the effective run CPU ceiling, the accounted per-writer working set, the destination
path's truthful safe range, and any explicit operator ceiling. An arbitrary product constant
MUST NOT be the default concurrency authority. Explicit configuration remains an upper bound,
never permission to exceed CPU or managed-memory admission.

The Parquet destination MUST compile deterministic consecutive object groups independently of
worker completion order and MAY execute several groups concurrently. Object ordinals, segment
membership, keys, manifests, acknowledgements, receipts, and package identity MUST be identical
at jobs 1 and jobs N. Encoding and durability/publication use their declared executor classes;
concurrency MUST remain bounded by the prepared writer count and accounted memory.

The default is evidence-seeking rather than maximum-thread seeking: parallelism expands only to
the admitted host/memory ceiling and is retained only when a focused release workload improves
or preserves wall time. A measured non-improving writer count is not selected by default.

## Alternatives Considered

- Keep staged destination pressure in the global jobs minimum and require `--jobs` to bypass it.
  Rejected because a downstream queue depth is not a CPU topology, makes the default
  non-work-conserving, and contradicts the stage-local backpressure contract.
- Hard-code a larger Parquet writer count. Rejected because useful concurrency depends on host,
  schema, codec, memory, and device pressure; another arbitrary constant merely moves the
  bottleneck.
- Let each destination create private worker pools or semaphores. Rejected because CPU and memory
  would escape the execution host and ledger, making oversubscription and extension behavior
  destination-specific.
- Remove all destination bounds. Rejected because bounded backpressure, deterministic rollback,
  and constant memory require exact stage-local item, byte, lane, and writer authorities.

## Consequences

Fast sources can continue decoding and packaging while a bounded destination stage applies local
backpressure. Scheduler reports must distinguish upstream jobs from destination-stage capacity.
Parquet may retain more durable segment handles and writer working sets, but both are planned and
accounted; no package-sized batch materialization is permitted.

The change requires focused jobs-invariance, cancellation, memory-release, and release-throughput
evidence. The 1 TiB run is not required to prove the topology repair; a bounded CPU-heavy
multi-object fixture can falsify parallel execution and performance before another large-host
promotion run.
