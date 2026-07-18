Status: active
Created: 2026-07-10
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/done/2026-07-07-interop-boundary-overhead-triage.md

# P3 WS-H: measured interop boundaries

## Scope

Measure and document Python PyCapsule/C Data Interface, subprocess Arrow IPC framing, row-shaped fallback, and the prospective WASM stream cost model. Preserve the rule that foreign rows become Arrow batches at the boundary and do not enter the engine runtime model.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md`
- `.10x/tickets/2026-07-11-p3-h2-python-incremental-arrow-boundary.md`
- `.10x/tickets/2026-07-11-p3-h3-subprocess-stream-supervision.md`
- `.10x/tickets/2026-07-11-p3-h4-wasm-cost-interface-model.md`
- `.10x/tickets/2026-07-11-p3-h5-interop-envelope-closeout.md`

## Acceptance criteria

- Python zero-copy is verified at batches of at least 1 MiB and its startup/per-batch costs are recorded.
- Subprocess Arrow IPC throughput and copy count are measured against the native path.
- Row-shaped compatibility costs are explicit rather than blended into Arrow-native claims.
- WASM's projected stream/sandbox cost model is recorded without pretending Tier 3 exists.
- Python and subprocess production boundaries are incremental, ledger-accounted, cancellable implementations of one neutral foreign-stream contract rather than eager private runtimes.

## Blockers

Blocked until WS-L supplies comparable measurement/reporting. No WASM implementation is authorized.

## References

- `.10x/decisions/neutral-foreign-stream-boundary.md`
- `.10x/research/2026-07-11-foreign-interop-boundary-audit.md`
- `.10x/specs/foreign-stream-interop.md`
