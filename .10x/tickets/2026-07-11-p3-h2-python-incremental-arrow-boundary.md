Status: open
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

# P3 H2: incremental Python Arrow/row boundary

## Scope

Replace `PythonBatchRead`/C-stream materialization in production with the neutral incremental producer, live memory/backpressure/cancellation/lane integration, real Arrow C lifetime/copy evidence, adaptive direct row conversion, shared reconciliation, and GIL/free-threaded measurement.

## Acceptance criteria

- Python resources can exceed memory by orders of magnitude while resident boundary memory remains budgeted.
- Real PyArrow C Array/Stream paths cover type/lifetime/error/cancellation cases and verified copy labels.
- Dict rows retain only one accounted conversion window and are never whole-resource JSON collections.
- Schema variance uses the shared contract policy; no Python-local competing schema truth remains.
- GIL/free-threaded concurrency uses declared execution-host lanes and produces deterministic fixed-input packages.

## Evidence expectations

Real interpreter/PyArrow matrices, memory traces, copy probes, backpressure/cancellation/error tests, package hashes, before/after throughput, and adversarial FFI/lifetime review.

## Explicit exclusions

No arbitrary Python execution inside engine operators or untrusted sandbox claim.

## Blockers

None. H1, A4, and A2 are done; this ticket is executable.

## References

- `.10x/specs/foreign-stream-interop.md`
