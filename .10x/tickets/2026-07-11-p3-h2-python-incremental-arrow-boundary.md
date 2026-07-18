Status: active
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

## Journal

- 2026-07-18 — Activated after H1 closeout. First slice migrated the internal Python iterator emission boundary onto IX1 `ForeignBatchOutcome` values. Existing materializing collectors now wrap the neutral outcome stream instead of owning a second Python-local semantics, and production `PythonResource::execute_stream` consumes neutral outcomes before sending ordinary runtime batches.
- 2026-07-18 — Removed the now-dead `visit_python_iterable` internal wrapper instead of suppressing dead-code warnings. The remaining internal boundary is `visit_python_foreign_iterable`; compatibility materialization still exists only through public collector APIs and remains to be burned down in later H2 slices.
- 2026-07-18 — Migrated direct JSON/dict-row helpers to the same neutral outcome visitor. `batches_from_json_dict_rows` is now a collector wrapper over `visit_json_dict_rows`; dict-row conversion has one batch-to-foreign-outcome conversion path shared with Python iterator execution.
- 2026-07-18 — Bounded audit of `pyo3-arrow 0.19.0` found import/export C Data surfaces and buffer-protocol comments, but no ready-made allocation/copy-proof signal. H2 must keep Arrow C outcomes `copy_unknown` until a real pointer/lifetime/allocation probe is implemented; do not infer zero-copy from use of the crate alone.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-python bridge --lib --locked -j 12` — passed. Covers neutral foreign outcome emission for dict rows and verifies downstream stop still halts the Python generator before exhaustive materialization.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-python direct_dict_rows_emit_neutral_outcomes_without_collecting_all_batches --lib --locked -j 12` — passed. Confirms direct dict-row compatibility exits through the neutral outcome visitor and can stop after the first bounded row window.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-python --lib --tests --locked -j 12 -- -D warnings` — passed. This caught and forced removal of the superseded internal batch wrapper.

## Review

Partial pass for the neutral-boundary migration slices. This does not close H2: real PyArrow C Array/Stream lifetime/copy matrices, memory traces, and throughput evidence are still outstanding. No production batch size/default changed and no hot path was made more conservative. The pyo3-arrow audit specifically prevents a false zero-copy claim.

## Retrospective

The production resource path was already closer to the target than the ticket text suggested: it streamed batches through the injected blocking lane. The architecture gap was that the internal bridge still spoke Python-local `Batch`/`PythonYieldKind` rather than the neutral foreign contract. Migrating the internal emission point first keeps the later FFI/copy-proof work source-agnostic. The remaining work is hard evidence, not plumbing: prove or deny Arrow C zero-copy with actual buffers and then remove/reshape the remaining materializing preview/DLT collectors.
