Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Injected execution host and runtime ownership

## Context

CDF must run standalone at full hardware utilization and embed under other process/runtime owners. Current sources/destinations construct private Tokio runtimes or call `block_on`, making thread count, cancellation, memory, and I/O concurrency invisible. Exporting Tokio types through extension contracts would replace one leak with another.

## Decision

`cdf-runtime` defines a runtime-agnostic `ExecutionHost` and run-scoped `ExecutionServices` contract using boxed futures and opaque handles. It exports no Tokio, Rayon, DataFusion, DuckDB, Python, or product types. `cdf-engine` owns CDF's default standalone implementation; embedders may supply another implementation satisfying conformance.

Services include:

- async control/I/O task scope;
- bounded CPU submission and CPU-slot admission;
- bounded blocking/FFI lane submission;
- run cancellation/deadline and structured child scope;
- the neutral memory coordinator;
- clock/telemetry hooks that remain outside artifact identity.

The standalone product creates exactly one host at its composition root. Library/source/destination crates MUST NOT create runtimes, use global runtime singletons, or call `block_on` in production. Async operations remain async to the host boundary.

The default architecture is a Tokio control/I/O runtime plus a distinct bounded CPU executor. The exact CPU executor implementation is selected after WS-L comparison and dependency review; synchronous CPU work MUST NOT execute on I/O worker threads. DataFusion execution joins the same CPU/memory authority rather than starting an ungoverned competing pool.

Blocking/FFI lanes are driver-declared data, not destination-name branches. A lane declares stable id, maximum concurrency, CPU-slot cost/internal parallelism, affinity (`shared` or `pinned`), cancellation behavior, and whether interruption is safe. DuckDB/Python and future interpreted Python/WASM/native custom parsing use the same generic mechanism.

All run tasks are structured children. First terminal failure requests cancellation, stops new admission, drains or aborts staged work according to protocol, joins every child, and releases all memory/CPU/lane permits before the run returns. Detached tasks are forbidden.

`--jobs` caps admitted logical partitions. Effective concurrency is the minimum allowed by CPU slots, memory working sets, source rate/capabilities, destination writer/staging capabilities, and configured jobs. This resolved join is observable plan/run evidence.

Parallel outputs carry plan partition ordinal and local sequence into a memory-accounted reorder boundary. Scheduling completion order never reaches canonical segment/package identity.

## Alternatives considered

- `#[tokio::main]` or a global Tokio runtime in every library: rejected because embedding/nesting and ownership are wrong.
- Export Tokio `Handle` in driver traits: rejected because it couples extensions and future hosts to one executor.
- Use `spawn_blocking` for all CPU and FFI work without lane admission: rejected because Tokio's blocking pool is not a CPU/memory/concurrency policy.
- One Rayon/global CPU pool without host injection: rejected pending measurement and because embedders need ownership/control.
- Destination-specific worker threads: rejected because affinity/concurrency are generic lane capabilities.

## Consequences

Production `futures_executor::block_on`, private object-store runtimes, and destination-owned Tokio runtimes are migration targets. Tests gain deterministic host implementations. New sources/destinations declare execution needs and receive services without editing generic orchestration. P3 lab records CPU utilization, queue/wait time, oversubscription, and context switches per executor/lane.
