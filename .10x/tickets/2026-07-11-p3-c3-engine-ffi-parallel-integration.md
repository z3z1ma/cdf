Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md

# P3 C3: DataFusion, Python, and native parallelism integration

## Scope

Join DataFusion task execution, codec/native internal threads, DuckDB lanes, Python GIL/free-threaded modes, and subprocess/FFI work to shared CPU-slot admission; eliminate hidden oversubscription while preserving semantic equivalence.

## Acceptance criteria

- Profiles prove DataFusion/native work does not bypass claimed CPU authority.
- GIL builds interleave safely; free-threaded builds parallelize; packages/evidence match.
- Native thread settings and lane affinity are capability-driven and observable.
- CPU-bound paths saturate effective cores without runaway runnable threads/context switches.

## Evidence expectations

Thread/CPU profiles, DataFusion hook/confinement tests, Python build matrix, native library settings, context switches, cancellation/panic, and dependency review.

## Explicit exclusions

No distributed DataFusion/Ballista or WASM implementation.

## Blockers

Depends on C2 and A4.

## References

- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/specs/execution-host-structured-runtime.md`
