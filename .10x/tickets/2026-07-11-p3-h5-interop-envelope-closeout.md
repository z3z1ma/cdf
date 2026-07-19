Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/2026-07-11-p3-h2-python-incremental-arrow-boundary.md, .10x/tickets/2026-07-11-p3-h3-subprocess-stream-supervision.md, .10x/tickets/2026-07-11-p3-h4-wasm-cost-interface-model.md, .10x/tickets/2026-07-11-p3-f3-stress-generators-laws.md

# P3 H5: interop conformance and envelope closeout

## Scope

Run the shared foreign producer matrix, constant-memory/chaos/jobs laws, publish honest mode-specific envelope guidance, reconcile the interop triage, and close WS-H only from raw evidence.

## Acceptance criteria

- Implemented Python/subprocess modes pass shared semantics, memory, cancellation, redaction, recovery, and determinism conformance.
- Performance docs/sheets distinguish verified zero-copy, IPC, and row compatibility with host labels.
- WASM remains prospective/unknown where not executable.
- Interop triage is terminal by absorption and all claims link raw evidence.

## Evidence expectations

Full reports/profiles/copy proofs, stress/chaos/package hashes, generated envelope cells, docs diff, triage reconciliation, and adversarial performance/security review.

## Explicit exclusions

No Wasmtime host or native-speed guarantee for compatibility rows.

## Blockers

Depends on H2–H4 and F3.

## Journal

- 2026-07-19: H2's adversarial review assigned two program-level conformance cells here rather than hiding them at the Python adapter: preserve `ForeignBatchOutcome` transfer/copy telemetry through ordinary runtime batches into explain/run evidence, and prove Arrow C release callbacks execute exactly once across producer deletion, cancellation, downstream-thread destruction, and error paths. H2 supplies real >2 MiB PyArrow alias/lifetime/cross-thread evidence but does not claim these remaining shared telemetry/release cells.

## References

- `.10x/specs/foreign-stream-interop.md`
