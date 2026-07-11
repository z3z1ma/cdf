Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/specs/execution-host-structured-runtime.md

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

Depends on L5, DX1, and A2.

## References

- `.10x/decisions/injected-execution-host-runtime-ownership.md`
- `.10x/research/2026-07-11-execution-host-runtime-audit.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/runtime-memory-backpressure.md`
