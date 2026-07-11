Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/2026-07-07-interop-boundary-overhead-triage.md

# P3 WS-H: measured interop boundaries

## Scope

Measure and document Python PyCapsule/C Data Interface, subprocess Arrow IPC framing, row-shaped fallback, and the prospective WASM stream cost model. Preserve the rule that foreign rows become Arrow batches at the boundary and do not enter the engine runtime model.

## Acceptance criteria

- Python zero-copy is verified at batches of at least 1 MiB and its startup/per-batch costs are recorded.
- Subprocess Arrow IPC throughput and copy count are measured against the native path.
- Row-shaped compatibility costs are explicit rather than blended into Arrow-native claims.
- WASM's projected stream/sandbox cost model is recorded without pretending Tier 3 exists.

## Blockers

Blocked until WS-L supplies comparable measurement/reporting. No WASM implementation is authorized.
