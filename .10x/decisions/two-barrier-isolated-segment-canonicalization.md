Status: active
Created: 2026-07-20
Updated: 2026-07-20

# Two-barrier isolated segment canonicalization

## Context

CDF's canonical `_cdf_package_row_ord` is a dense package-global ordinal assigned after every row-selecting operation: decode quarantine, reconciliation, contract validation, derive/filter, late-data policy, deduplication, and limits. An isolated worker executing one partition cannot know that partition's package-global ordinal prefix while earlier partitions are still running. Letting each worker start at zero produces noncanonical segment bytes; reserving sparse ordinal ranges breaks destination range allocation; counting first and rereading the source doubles the expensive path; letting workers finalize packages violates the coordinator's existing commit authority.

The direct local engine already has the correct fused behavior and MUST remain the performance default. The isolated protocol needs a portable form of the unavoidable prefix barrier without moving data through scheduler control messages or redoing source work.

## Decision

Isolated multi-partition execution uses two typed, content-addressed stages under one coordinator-owned package attempt:

1. **Partition preparation.** A worker executes source read through every row-selecting semantic operation and fixes the canonical per-partition segment boundaries. It writes bounded, ledger-accounted prepared-segment artifacts containing the final typed rows without `_cdf_package_row_ord`, plus exact source, verdict, lineage, row, and byte evidence. Prepared artifacts are operational staging, not package identity, and are referenced—not embedded—in the result.
2. **Canonical segment finalization.** After preparation results are independently admitted in canonical partition order, the coordinator computes a checked dense prefix sum of accepted rows. It compiles a finalization task for each partition that binds the admitted prepared references and exact `package_row_ord_start`. Workers may finalize these tasks concurrently: they append the dense ordinal vector and encode the already-fixed canonical segments under the recorded package policy. They return fenced canonical-segment receipts only.

The coordinator verifies both result rounds, assembles identity and evidence in canonical order, finalizes the package, binds the destination, and alone advances the commit gate/checkpoint. A failed or stale first- or second-stage attempt cannot advance authority. Prepared staging is reclaimed only after final package settlement or proven lease expiry.

Direct local execution remains fused and creates no prepared artifact. Isolated/container/distributed hosts pay the operational staging cost because they cross a process/host boundary; they MUST NOT make the fused local path slower. A same-host implementation may retain an accounted prepared payload behind the same artifact authority, but the equivalence law MUST also exercise serialized durable bytes so it cannot depend on shared objects.

For finite streaming epochs, each bounded package epoch uses the same two barriers and resets the dense package ordinal at zero. Unbounded sources never accumulate an unbounded preparation frontier: package rotation, memory, spill, and control limits bound each epoch before preparation admission.

## Alternatives considered

- **Plan-time sparse ordinal ranges per partition.** Rejected because filtering makes actual cardinality unknown, sparse values defeat contiguous destination key-range allocation, and fixed-width reservations can overflow.
- **Pre-count, then reread and execute.** Rejected because it doubles source I/O and row-selecting work on the isolated path, violating the performance doctrine and single-extraction rule.
- **Remove package ordinals from segments.** Rejected because canonical provenance is destination-neutral package evidence and destinations must not reconstruct different row identities.
- **Rewrite final segments in the coordinator.** Rejected as the only product shape because it centralizes data and encoding on one node. The coordinator may execute a finalization task locally, but it uses the same worker contract and can distribute the tasks.
- **Let each partition worker publish a package.** Rejected because package identity, destination receipt, settlement, and checkpoint authority would fork across workers.

## Consequences

The portable worker protocol gains explicit preparation and finalization task/result authority rather than overloading one ambiguous capsule. Prepared artifacts add one bounded IPC staging encode/decode for isolated execution; they add no work to the direct local path. The second barrier preserves parallel final encoding because only the prefix scan is centralized and metadata-sized. Package bytes, hashes, verdicts, receipts, and checkpoints remain identical to direct execution, while future Spark/Flink/Ballista/container hosts can schedule both stages without interpreting CDF semantics.

P3 C5 owns the local serialized equivalence law and cleanup/failure matrices for both barriers. Later distributed work owns transport, placement, remote lease services, and worker daemons around these unchanged semantics.
