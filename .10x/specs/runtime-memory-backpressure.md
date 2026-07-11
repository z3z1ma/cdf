Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Runtime memory, backpressure, and spill

## Purpose and scope

This specification governs process/managed/spill budgets, shared DataFusion accounting, accounted payloads, async byte admission, deadlock prevention, sub-budgets, spill identities, diagnostics, and constant-memory conformance.

## Budget model

Every run MUST resolve one process RSS budget and one smaller managed-memory pool after versioned native/runtime headroom. The resolution MUST use effective cgroup/container constraints when present and MUST be plan/run evidence. Spill disk budget is separate and MUST be checked before writing.

Every live CDF-owned data buffer and material operator state MUST have one named reservation in the shared pool through the neutral `cdf-memory` contract, including decode/decompression windows, Arrow payloads, transform scratch/output, validation/dedup state, queues, package encoding, quarantine buffers, remote staging, and destination staging. Extension crates MUST NOT depend on DataFusion memory types. Small control metadata MAY be aggregated by a named consumer but is never unaccounted.

## Accounted payload law

Runtime stage interfaces MUST use accounted envelopes. Cloning a shared payload shares its lease; allocating or deep-copying payload bytes requires a new reservation. Reservations MUST release on normal completion, error, cancellation, panic unwind, channel close, and aborted destination sessions.

The executor MUST reserve a source's declared maximum poll/decode working set before polling legacy/custom streams and reconcile the returned payload at the source boundary. CDF-owned optimized decoders SHOULD reserve granular targets internally through the neutral handle. Transform operators MUST account for simultaneous input, scratch, and output peaks. Package/destination readers MUST yield bounded accounted batches or streams rather than package-sized collections.

## Admission and backpressure

Operators MUST declare a minimum working set. Jobs/partitions are admitted only when the coordinator can reserve that set under global and applicable sub-caps. A task may wait while holding memory only when it can release memory/progress without another allocation; cyclic hold-and-wait is a conformance failure.

On pressure, owners MUST apply flush, backpressure/release, spill, then clean failure. Non-pausable sources MUST declare a spill policy before execution. `--jobs` is a ceiling, not a promise; effective concurrency is the capability/CPU/memory/destination join and is reported.

## Discovery metadata

Discovery MUST enforce 64 MiB per file, 128 MiB concurrent total, and 8 probes by default through weighted permits inside the global pool. Actual small probes may use all eight slots; large probes reduce concurrency. File count is unbounded by this working-set cap. Configuration/evidence remain deterministic and sampling semantics remain independent.

## Spill

Every spill artifact MUST have typed owner/run/operator/partition identity, schema/content identity where data-bearing, byte count, lifecycle, and cleanup authority. Spill MUST NOT enter package identity unless later consumed into canonical package bytes through the ordinary writer. Repeated cleanup is safe. Disk exhaustion fails cleanly before uncontrolled writes.

## Diagnostics and conformance

The coordinator MUST record current/peak bytes by consumer/class, waits, flushes, spill bytes/counts, effective concurrency, and largest consumers. `--explain-memory` and run reports render these facts after redaction.

Permanent tests MUST cover shared clones, allocating transforms, cancellation/error cleanup, queue closure, unpausable-source spill, exact-row dedup spill, discovery weighted permits, destination staging, too-small working set, deadlock adversaries, input-size-invariant RSS, and DataFusion/non-DataFusion competition.

Source/resource conformance MUST falsify declared poll/decode working sets. A mock external source and destination MUST use the neutral contract without a DataFusion dependency; the default executor and a deterministic test coordinator MUST satisfy the same laws.

## Explicit exclusions

This spec does not make RSS an allocation primitive, treat spill as evidence, guarantee configured `--jobs`, or authorize unbounded native-library allocations outside measured headroom.
