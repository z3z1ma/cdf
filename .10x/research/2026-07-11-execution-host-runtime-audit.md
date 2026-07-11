Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Execution host and runtime ownership audit

## Question

How should CDF saturate CPU/I/O safely while remaining embeddable in containers and future Spark/Flink hosts, and where do current library crates create or block runtimes themselves?

## Sources and methods

Searched runtime, source, destination, CLI, engine, and conformance code/Cargo graphs for Tokio, futures-executor, `block_on`, runtime construction, blocking threads, and pool dependencies. Inspected object-store transport and Parquet destination runtime ownership, CLI entry, and current async project APIs.

## Findings

The production CLI calls `futures_executor::block_on` inside command modules. Engine/project public execution is async, but the ordinary product path has no Tokio host or partition task graph.

Library crates own hidden runtimes:

- file transport lazily constructs a global multi-thread Tokio runtime in `OnceLock`, spawns a future, then blocks on a synchronous channel;
- Parquet destination constructs a current-thread Tokio runtime per destination and calls `Runtime::block_on` around object-store operations;
- several local/transport helpers use `futures_executor::block_on` around object-store futures;
- destination kernel sessions are synchronous, forcing async object stores behind blocking bridges;
- Tokio manifest versions vary while Cargo resolves one newer version transitively.

These patterns create nested-runtime hazards, unbudgeted thread pools, invisible concurrency, context switches, and embedding failures. They also prevent a single host from joining CPU, memory, destination, and I/O limits.

DataFusion and object-store dependencies already bring Tokio transitively, but exporting Tokio handles through destination traits would make extension APIs host-specific. Blocking drivers have different needs: DuckDB/Python may require bounded or affinity-preserving lanes; Postgres/object stores should become async/bulk where possible. A closed enum or destination-name match in the scheduler would repeat the architecture problem already assigned to DX.

## Conclusion

`cdf-runtime` should define a runtime-agnostic `ExecutionHost`/services contract using boxed futures and opaque task/CPU/blocking-lane handles, without Tokio/Rayon/DataFusion types. `cdf-engine` supplies CDF's default standalone implementation and composes the same DataFusion memory pool. Project/CLI inject the host; extension drivers receive services through resolution/session context.

The default standalone host uses Tokio for control/I/O and a separately bounded CPU executor for decode/validate/encode. The exact CPU executor dependency/implementation is chosen from WS-L measurements and supply-chain review, but its contract is fixed. Blocking/FFI submissions carry driver-declared lane identity, concurrency, CPU-slot cost, and optional thread affinity. No library crate creates a runtime or calls `block_on` after migration.

Structured concurrency owns every task: first terminal error cancels siblings, all tasks join/abort cleanly, accounted buffers drop, staged ingress aborts, and no detached work can outlive the run. Tagged partition outputs enter a memory-bounded deterministic reorder/segment assembler.

CPU slots are a shared budget, not merely thread count. External/native internal parallelism must consume declared slots or be configured down, preventing DataFusion, DuckDB, compression, and Python from each saturating all cores simultaneously.

## Limits

This audit does not choose Rayon versus a dedicated Tokio/other compute pool. A4 must benchmark the candidates on decode/validation/DataFusion/native-driver workloads before ratifying a new dependency. Existing test-only `futures_executor::block_on` may remain as compatibility scaffolding if production/static gates prove it cannot enter product paths.
