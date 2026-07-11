Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Execution host and structured runtime

## Purpose and scope

This specification governs runtime ownership/injection, I/O and CPU separation, blocking/FFI lanes, CPU-slot admission, cancellation/structured concurrency, effective jobs, deterministic reordering, and embedding conformance.

## Host contract

All production runs MUST receive `ExecutionServices` from an explicit host. The neutral contract MUST expose no concrete executor/runtime implementation type. Standalone CLI/container composition creates the default host once; embedded callers may provide an existing conforming host.

No production source, destination, format, package, project, or CLI command module may construct a Tokio runtime, access a global runtime singleton, or call a blocking executor around an async future. Static architecture tests MUST enforce this outside approved standalone/test modules.

## Executor classes

Control/I/O tasks MUST run on the async I/O executor and MUST yield rather than perform long CPU/blocking work. Decode, validation, normalization, compression/encoding, hashing where parallelized, and other CPU kernels MUST run on the bounded CPU executor with declared slot cost.

Blocking/FFI operations MUST use a declared lane. Pinned lanes preserve thread affinity through a session actor; shared lanes use bounded workers. Lane limits and native internal parallelism participate in CPU admission. Unsafe interruption MUST be refused; cancellation becomes stop-admission plus cooperative completion/rollback.

## Structured concurrency

Every child task belongs to one run scope. Cancellation/deadline/error MUST stop new work and deterministically join or abort all children. Memory leases, CPU slots, lane permits, temporary/spill artifacts, and staged destination ingress MUST be released or handed to explicit recovery authority before return.

Panics/task-join failures map to `Internal` errors with redacted context and cannot be swallowed. A success event/receipt/checkpoint cannot occur while required child work remains detached.

## Scheduling and determinism

Effective jobs MUST join configured ceiling, CPU slots, memory admission, source limits, destination limits, and scope single-writer rules. The join and pressure reductions are reported but do not enter package identity.

Partition results MUST be tagged by plan ordinal and local deterministic sequence. Reorder buffering is memory-accounted and applies backpressure; canonical assembly follows plan order regardless of completion order.

## Conformance and performance

Permanent tests MUST cover standalone host, already-running Tokio embedding, deterministic test host, cancellation at every stage, panic/join failure, pinned/shared lanes, slow/blocking destination, CPU oversubscription, no-private-runtime static gate, jobs=1/N identity, and task/memory/permit leak detection.

The lab MUST compare candidate CPU executor implementations before dependency ratification and measure CPU utilization, runnable/blocked time, context switches, queue wait, lane saturation, and I/O overlap. Default tuning MUST saturate available CPU for CPU-bound paths without oversubscribing native libraries.

## Explicit exclusions

This spec does not implement distributed scheduling, expose Tokio/Rayon handles to extensions, promise configured jobs, or permit detached background cleanup without durable ownership.
