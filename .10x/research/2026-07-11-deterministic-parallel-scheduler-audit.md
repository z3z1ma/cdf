Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Deterministic parallel scheduler audit

## Question

How can CDF saturate CPU/I/O across partitions and format units while preserving canonical limits/order/positions, file completion, retries, package identity, checkpoint scopes, and native-library CPU budgets?

## Sources and methods

Inspected kernel scan/partition/capability types, engine planner/execution, file/REST partition behavior, hidden REST rate/retry loops, project checkpoint/file-manifest planning, source/format/host/memory/segmentation specs, and P3 concurrency requirements.

## Findings

`ScanPlan.partitions` is an ordered vector but carries no explicit ordinal, working set, retry, speculation, or concurrency authority. Engine execution loops it sequentially and uses one mutable `remaining_limit` after residual filters and before contract evaluation, so current limit means a canonical candidate-row prefix and may yield fewer accepted rows after quarantine. Segment ids are encounter order.

Files are logical partitions with immutable identity/attestation. Future Parquet row groups/ORC stripes/Avro blocks are units inside a file; independent concurrency cannot mark a file processed until all selected units succeed. REST currently runs retry/rate logic inside one resource partition, creating driver-local timing/concurrency state invisible to a global scheduler.

Parallel completion cannot feed package assembly directly. A slow early partition with fast later partitions needs a bounded reorder frontier; otherwise memory grows or scheduler completion changes rows/segments. Global `LIMIT` is especially subtle: later partitions may be speculatively read but only the canonical prefix at the compiled limit operator counts, and discarded speculative work cannot advance source state.

Nested parallelism can oversubscribe heavily: file partitions, row groups, DataFusion, compression, DuckDB, and Python/native libraries may each use all cores. Thread count is not enough; the injected execution host's CPU-slot/internal-parallelism declarations must arbitrate.

Checkpoint scope is a stronger serialization boundary than destination writer count. Two transitions deriving from the same scope/head cannot independently commit. Different resource/scope extraction may overlap under global budgets, but final package/receipt/checkpoint ordering remains explicit.

## Conclusion

Compile stable partition/unit ordinals and scheduling declarations into the execution graph. Use global hierarchical admission, work-conserving execution, and a byte/accounted canonical commit frontier. Effective jobs is a runtime capability join outside package identity; rows/positions/evidence cross the frontier only in plan order.

Partition retry, rate/quota, speculation, and source identity are capabilities. Hidden driver pools/retry loops migrate to injected services. Nested work consumes shared CPU slots. Scope locks serialize state transitions without serializing all upstream work.

## Limits

WS-L/C children must calibrate default jobs/reorder lookahead/fairness and integrate DataFusion's actual executor behavior. This audit does not authorize distributed workers.
