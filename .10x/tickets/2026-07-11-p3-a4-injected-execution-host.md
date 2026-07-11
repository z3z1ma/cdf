Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/specs/execution-host-structured-runtime.md

# P3 A4: injected execution host and structured runtime

## Scope

Add neutral execution-host/services and blocking-lane contracts, select/implement the measured standalone Tokio+CPU host, inject it through CLI/project/engine/source/destination composition, migrate production private runtimes/blocking bridges, and establish structured cancellation/task ownership. Do not yet build the full concurrent operator graph.

## Acceptance criteria

- WS-L benchmark and supply-chain evidence select the CPU executor; no dependency is added by taste.
- Standalone and already-running-Tokio embedding execute without nested-runtime panic/deadlock.
- Production static gates find no runtime construction/global singleton/`block_on` outside the composition root.
- Object-store transport and Parquet destination use injected async services; blocking/FFI drivers use generic declared lanes.
- Cancellation/panic/error joins all tasks and releases memory/CPU/lane permits.
- CPU-slot admission prevents configured native/internal parallelism from oversubscribing effective cores.
- A mock new source/destination declares execution needs without editing scheduler/orchestration code.

## Evidence expectations

Executor comparison, dependency review, standalone/embedded tests, static runtime-ownership gate, cancellation/panic/Loom tests where practical, CPU/context-switch profiles, redaction and artifact invariance, and adversarial embedding review.

## Explicit exclusions

No full decode-to-destination channels, production partition fan-out, adaptive segment assembler integration, or throughput target closure.

## Blockers

None. L5, DX1, and A2 are complete.

## Progress and notes

- 2026-07-11: Added the neutral `ExecutionHost`/`ExecutionServices`/run-scope contract in `cdf-runtime`, with structured I/O, CPU, and blocking submissions; cooperative cancellation/join; CPU/native-parallelism cost declarations; affinity/interruption-safe blocking lanes; shared memory access; and report telemetry. The serialized capability model contains no Tokio/DataFusion/DuckDB/Python/destination identity and the runtime dependency gate remains clean.
- 2026-07-11: Implemented the engine-owned standalone host: one Tokio 1.52.3 I/O runtime, a separate fixed CPU pool, capability-declared blocking pools, and one global CPU-slot semaphore shared by CPU and lane workers. Structured scopes join I/O/CPU/lane completions, map panics to internal errors, cancel on first terminal failure, and release captured memory leases. Standalone and already-running-Tokio embedding tests pass.
- 2026-07-11: The release executor comparison measured the fixed pool at 1.0128x Tokio `spawn_blocking` wall time for the recorded SHA-256 workload. `.10x/decisions/standalone-cpu-executor-v1.md` selects the fixed pool because throughput is equivalent while admission/isolation are stronger and no new CPU dependency is added.
- 2026-07-11: Injected `ExecutionServices` through the project run boundary and production `cdf run`. The CLI composition branch creates exactly one standalone host, resolves a container-aware managed budget, and drives the async project future through that host rather than command-local `futures_executor::block_on`. Existing library/test callers retain an explicit compatibility path while remaining migration targets.
- 2026-07-11: Extended the composition-root host injection to `preview` and executable `backfill`; planning-only backfill does not allocate a host. The host now owns the temporary compatibility poller required by synchronous SQL/REST drivers that still create private runtimes. Focused REST and Postgres preview tests prove this boundary avoids nested-runtime panics while async tasks remain explicitly submitted to the host I/O runtime. Driver-owned runtimes remain an acceptance blocker for this ticket, not hidden CLI behavior.
- 2026-07-11: Added a runtime-neutral typed `ExecutionServices::run_io` bridge for synchronous extension methods. The neutral trait carries boxed futures/results without Tokio types; the standalone host submits them to its owned I/O runtime and synchronously joins the result. An embedding test invokes the bridge from inside an unrelated current-thread Tokio runtime without panic or deadlock. This is the generic boundary needed to delete destination-owned object-store runtimes without destination-name scheduler branches.
- 2026-07-11: Removed the Parquet destination's private Tokio dependency and per-instance current-thread runtime. Destination resolution now carries optional neutral execution services; production run/backfill inject them, while planning remains side-effect-free without allocating a host. All object-store put/get/head/delete operations use the generic host bridge. The full 27-test Parquet suite, a CLI filesystem destination run, targeted checks, strict Clippy, and the no-Tokio static search pass; evidence is `.10x/evidence/2026-07-11-p3-a4-parquet-host-io.md`.
- 2026-07-11: Removed declarative file transport's global lazy Tokio runtime. Object-store list/head/range now use injected `ExecutionServices`, threaded through production run, preview, plan/explain, executable backfill, discovery, and file-resource construction. Local/HTTP transports require no executor. Focused object-store, recursive-glob, CLI run, checks, strict Clippy, and static runtime searches pass; evidence is `.10x/evidence/2026-07-11-p3-a4-file-transport-host-io.md`. Remaining command composition surfaces are recorded as the next A4 slice.

## References

- `.10x/decisions/injected-execution-host-runtime-ownership.md`
- `.10x/research/2026-07-11-execution-host-runtime-audit.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/runtime-memory-backpressure.md`
